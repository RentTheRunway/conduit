package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.impl.AMQImpl;
import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AMQPTransportTest {
    Channel channel;
    AMQPTransport amqpTransport;
    AMQPPublishProperties properties;
    AMQPMessageBundle messageBundle;

    private static final AMQPConnectionProperties CONNECTION_PROPS = new AMQPConnectionProperties(
            "BOB",
            "BOBS PASSWORD",
            "BOBS VHOST",
            1337,
            666,
            true);

    @Before
    public void before() {
        amqpTransport = spy(new AMQPTransport(false, "host", 1234, null));
        channel = mock(Channel.class);
        amqpTransport.setChannel(channel);

        properties = mock(AMQPPublishProperties.class);
        messageBundle = mock(AMQPMessageBundle.class);
    }

    @Test
    public void testConfirmModeDisabled() throws Exception {
        when(properties.isConfirmEnabled()).thenReturn(false);

        amqpTransport.publishImpl(messageBundle, properties);

        verify(channel, times(0)).confirmSelect();
        verify(channel, times(0)).waitForConfirms(anyLong());
    }

    @Test
    public void testConfirmModeEnabled() throws Exception {
        long timeout = 9876;
        when(properties.getTimeout()).thenReturn(timeout);
        when(properties.isConfirmEnabled()).thenReturn(true);
        when(channel.waitForConfirms(anyLong())).thenReturn(false);

        boolean result = amqpTransport.publishImpl(messageBundle, properties);

        assertFalse(result);
        verify(channel).confirmSelect();
        verify(channel).waitForConfirms(timeout);
    }

    @Test
    public void testListenImplDynamicQueues() throws IOException {
        String randoq = "randoq";
        String exchange = "exchange";
        String router = "router";

        AMQImpl.Queue.DeclareOk ok = mock(AMQImpl.Queue.DeclareOk.class);
        when(ok.getQueue()).thenReturn(randoq);
        when(channel.queueDeclare()).thenReturn(ok);
        amqpTransport.setChannel(channel);

        AMQPConsumerCallback consumerCallback = mock(AMQPConsumerCallback.class);

        AMQPCommonListenProperties commonListenProperties = AMQPSyncConsumerBuilder.builder()
                .callback(consumerCallback)
                .dynamicQueueCreation(true)
                .exchange(exchange)
                .dynamicQueueRoutingKey(router)
                .poisonQueueEnabled(Boolean.TRUE)
                .prefetchCount(1)
                .buildListenProperties();

        amqpTransport.listenImpl(commonListenProperties);
        verify(amqpTransport).getConsumer(consumerCallback, commonListenProperties, "." + randoq);
        verify(amqpTransport).createDynamicQueue(exchange, router, true);
        verify(channel).basicConsume(eq(randoq), eq(false), any(Consumer.class));
        verify(channel).basicQos(1);
    }

    @Test
    public void testListenImplDynamicQueuesPurgeOnConnect() throws IOException {
        String randoq = "randoq";
        String exchange = "exchange";
        String router = "router";

        AMQImpl.Queue.DeclareOk ok = mock(AMQImpl.Queue.DeclareOk.class);
        when(ok.getQueue()).thenReturn(randoq);
        when(channel.queueDeclare()).thenReturn(ok);
        amqpTransport.setChannel(channel);

        AMQPConsumerCallback consumerCallback = mock(AMQPConsumerCallback.class);
        AMQPCommonListenProperties commonListenProperties = AMQPSyncConsumerBuilder.builder()
                .callback(consumerCallback)
                .dynamicQueueCreation(true)
                .exchange(exchange)
                .purgeOnConnect(Boolean.TRUE)
                .dynamicQueueRoutingKey(router)
                .poisonQueueEnabled(Boolean.TRUE)
                .prefetchCount(1)
                .buildListenProperties();

        amqpTransport.listenImpl(commonListenProperties);
        verify(channel).queuePurge(randoq);
        verify(amqpTransport).getConsumer(consumerCallback, commonListenProperties, "." + randoq);
        verify(amqpTransport).createDynamicQueue(exchange, router, true);
        verify(channel).basicConsume(eq(randoq), eq(false), any(Consumer.class));
        verify(channel).basicQos(1);
    }

    @Test
    public void testListenImplBasicConfig() throws IOException {
        String queue = "randoq";
        String exchange = "exchange";
        String routingKey = "routingKey";
        AMQPConsumerBuilder.ExchangeType exchangeType = AMQPConsumerBuilder.ExchangeType.DIRECT;

        AMQImpl.Queue.DeclareOk ok = mock(AMQImpl.Queue.DeclareOk.class);
        when(ok.getQueue()).thenReturn(queue);
        when(channel.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), anyMap())).thenReturn(ok);
        amqpTransport.setChannel(channel);

        AMQPConsumerCallback consumerCallback = mock(AMQPConsumerCallback.class);

        AMQPCommonListenProperties commonListenProperties = AMQPSyncConsumerBuilder.builder()
                .callback(consumerCallback)
                .autoCreateAndBind(exchange, exchangeType, queue, routingKey)
                .poisonQueueEnabled(Boolean.TRUE)
                .prefetchCount(1)
                .buildListenProperties();

        amqpTransport.listenImpl(commonListenProperties);
        verify(amqpTransport).getConsumer(consumerCallback, commonListenProperties, "");
        verify(amqpTransport).autoCreateAndBind(exchange, exchangeType.toString(), queue, routingKey, true);

        verify(channel, times(1)).exchangeDeclare(exchange, exchangeType.toString(), true);
        verify(channel, times(1)).queueDeclare(queue, true, false, false, null);
        verify(channel, times(1)).queueBind(queue, exchange, routingKey);
        verify(channel, times(1)).queueDeclare(queue + AMQPTransport.POISON, true, false, false, null);
        verify(channel, times(1)).queueBind(queue + AMQPTransport.POISON, exchange, routingKey + AMQPTransport.POISON);

        verify(channel).basicConsume(eq(queue), eq(false), any(Consumer.class));
        verify(channel).basicQos(1);
    }


    @Test
    public void testClose_PrivateConnection_DisconnectsConnection() throws IOException {
        ConnectionFactory factory = mock(ConnectionFactory.class);
        int expectedTimeout = 5;
        when(factory.getConnectionTimeout()).thenReturn(expectedTimeout);
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport.setConnection(connection);

        amqpTransport.closeImpl();
        verify(connection).disconnect();
    }


    @Test
    public void testClose_SharedConnection_DoesntDisconnectConnectionButClosesOpenChannel() throws IOException, TimeoutException {
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport = spy(new AMQPTransport(connection));
        when(channel.isOpen()).thenReturn(true);
        amqpTransport.setChannel(channel);

        amqpTransport.closeImpl();
        verify(connection, never()).disconnect();
        verify(channel).close();
    }

    @Test
    public void testConnect_PrivateConnection_ConnectsAndCreatesChannel() throws IOException, TimeoutException {

        amqpTransport = new AMQPTransport(false, "host", 1234, null);
        AMQPConnection connection = mock(AMQPConnection.class);
        amqpTransport.setConnection(connection);

        amqpTransport.connect(CONNECTION_PROPS);
        verify(connection).connect(CONNECTION_PROPS);
        verify(connection).createChannel();
    }


    @Test
    public void testConnect_SharedConnection_JustCreatesChannel() throws IOException, TimeoutException {
        AMQPConnection connection = mock(AMQPConnection.class);


        amqpTransport = new AMQPTransport(connection);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport.connect(CONNECTION_PROPS);
        verify(connection, never()).connect(CONNECTION_PROPS);
        verify(connection).createChannel();
    }



    @Test
    public void testClose_SharedConnectionAndClosedChannel_DoesNothing() throws IOException, TimeoutException {
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport = spy(new AMQPTransport(connection));
        when(channel.isOpen()).thenReturn(false);
        amqpTransport.setChannel(channel);

        amqpTransport.close();
        verify(connection, never()).disconnect();
        verify(channel, never()).close();
    }
    @Test
    public void testStop_PrivateConnection_ClosesChannelStopsConnectionListening() throws IOException, TimeoutException {
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport.setConnection(connection);
        when(channel.isOpen()).thenReturn(true);
        amqpTransport.setChannel(channel);

        amqpTransport.stop();
        verify(channel).close();
        verify(connection).stopListening();
    }

    @Test
    public void testStop_PrivateConnectionClosedChannel_ClosesChannelStopsConnectionListening() throws IOException, TimeoutException {
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport.setConnection(connection);
        when(channel.isOpen()).thenReturn(false);
        amqpTransport.setChannel(channel);

        amqpTransport.stop();
        verify(channel, never()).close();
        verify(connection).stopListening();
    }

    @Test
    public void testStop_SharedConnection_JustClosesOpenChannel() throws IOException, TimeoutException {
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport = new AMQPTransport(connection);
        when(channel.isOpen()).thenReturn(true);
        amqpTransport.setChannel(channel);

        amqpTransport.stop();
        verify(channel).close();
        verify(connection, never()).stopListening();
    }

    @Test
    public void testStop_SharedConnectionAndClosedChannel_DoesNothing() throws IOException, TimeoutException {
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport = new AMQPTransport(connection);
        when(channel.isOpen()).thenReturn(false);
        amqpTransport.setChannel(channel);

        amqpTransport.stop();
        verify(channel, never()).close();
        verify(connection, never()).stopListening();
    }

    @Test
    public void testIsStopped_PrivateConnection_WaitsForConnectionToStopListening() throws InterruptedException {
        Duration wait=Duration.ofMillis(666);
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);
        amqpTransport.setConnection(connection);
        when(connection.waitToStopListening(wait)).thenReturn(true);
        Assert.assertTrue(amqpTransport.isStopped(666));
        verify(connection).waitToStopListening(wait);

        when(connection.waitToStopListening(wait)).thenReturn(false);
        Assert.assertFalse(amqpTransport.isStopped(666));
    }

    @Test
    public void testIsStopped_SharedConnection_JustChecksIfChannelIsOpen() throws InterruptedException {
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport = new AMQPTransport(connection);
        when(channel.isOpen()).thenReturn(true);
        amqpTransport.setChannel(channel);
        Assert.assertFalse(amqpTransport.isStopped(666));

        when(channel.isOpen()).thenReturn(false);
        amqpTransport.setChannel(channel);
        Assert.assertTrue(amqpTransport.isStopped(666));

        amqpTransport.setChannel(null);
        Assert.assertTrue(amqpTransport.isStopped(666));
    }

}
