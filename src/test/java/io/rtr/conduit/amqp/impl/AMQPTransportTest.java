package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.impl.AMQImpl;
import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

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
    AMQPConsumerCallback consumerCallback = mock(AMQPConsumerCallback.class);

    private static String MOCK_QUEUE = "randoq";
    private static String MOCK_EXCHANGE = "exchange";
    private static String TEST_ROUTING_KEY = "router";


    private static final AMQPConnectionProperties CONNECTION_PROPS = new AMQPConnectionProperties(
            "BOB",
            "BOBS PASSWORD",
            "BOBS VHOST",
            1337,
            666,
            true);

    private AMQPSyncConsumerBuilder dynamicQueueListenProperties(boolean poisonQueue, boolean purge) {

        return AMQPSyncConsumerBuilder.builder()
                .callback(consumerCallback)
                .dynamicQueueCreation(true)
                .exchange(MOCK_EXCHANGE)
                .dynamicQueueRoutingKey(TEST_ROUTING_KEY)
                .poisonQueueEnabled(poisonQueue)
                .purgeOnConnect(purge)
                .prefetchCount(1);

    }

    @Before
    public void before() throws IOException {
        amqpTransport = spy(new AMQPTransport(false, "host", 1234, null));
        channel = mock(Channel.class);

        AMQImpl.Queue.DeclareOk ok = mock(AMQImpl.Queue.DeclareOk.class);
        when(ok.getQueue()).thenReturn(MOCK_QUEUE);
        when(channel.queueDeclare()).thenReturn(ok);

        amqpTransport.setChannel(channel);

        consumerCallback = mock(AMQPConsumerCallback.class);

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

        amqpTransport.setChannel(channel);

        AMQPCommonListenProperties commonListenProperties = dynamicQueueListenProperties(true, false)
                .buildListenProperties();

        amqpTransport.listenImpl(commonListenProperties);
        verify(amqpTransport).getConsumer(consumerCallback, commonListenProperties, "." + MOCK_QUEUE);
        verify(amqpTransport).createDynamicQueue(MOCK_EXCHANGE, TEST_ROUTING_KEY, true);
        verify(channel).addShutdownListener(any(AMQPTransport.DynamicQueueCleanupShutdownListener.class));
        verify(channel).basicConsume(eq(MOCK_QUEUE), eq(false), any(Consumer.class));
        verify(channel).basicQos(1);
    }

    @Test
    public void testListenImplDynamicQueuesPurgeOnConnect() throws IOException {
        AMQPCommonListenProperties commonListenProperties = dynamicQueueListenProperties(true, true)
                .buildListenProperties();

        amqpTransport.listenImpl(commonListenProperties);
        verify(channel).queuePurge(MOCK_QUEUE);
        verify(amqpTransport).getConsumer(consumerCallback, commonListenProperties, "." + MOCK_QUEUE);
        verify(amqpTransport).createDynamicQueue(MOCK_EXCHANGE, TEST_ROUTING_KEY, true);
        verify(channel).addShutdownListener(any(AMQPTransport.DynamicQueueCleanupShutdownListener.class));
        verify(channel).basicConsume(eq(MOCK_QUEUE), eq(false), any(Consumer.class));
        verify(channel).basicQos(1);
    }

    @Test
    public void testListenImplDynamicQueues_ThrowsOnBind_StillSetsUpShutdownListener() throws IOException {
        when(channel.queueBind(anyString(), anyString(), anyString(), anyMap())).thenThrow(new RuntimeException());

        AMQPCommonListenProperties commonListenProperties = dynamicQueueListenProperties(false, false)
                .buildListenProperties();

        amqpTransport.listenImpl(commonListenProperties);
        verify(channel).addShutdownListener(any(AMQPTransport.DynamicQueueCleanupShutdownListener.class));
    }

    @Test
    public void testListenImplDynamicQueues_ThrowsOnDeclare_StillSetsUpShutdownListener() throws IOException {
        AMQPConsumerCallback consumerCallback = mock(AMQPConsumerCallback.class);
        AMQPCommonListenProperties commonListenProperties = AMQPSyncConsumerBuilder.builder()
                .callback(consumerCallback)
                .dynamicQueueCreation(true)
                .exchange(MOCK_EXCHANGE)
                .dynamicQueueRoutingKey(TEST_ROUTING_KEY)
                .poisonQueueEnabled(Boolean.TRUE)
                .prefetchCount(1)
                .buildListenProperties();

        amqpTransport.listenImpl(commonListenProperties);
        verify(channel).addShutdownListener(any(AMQPTransport.DynamicQueueCleanupShutdownListener.class));
    }

    @Test
    public void testListenImplBasicConfig() throws IOException {
        AMQPConsumerBuilder.ExchangeType exchangeType = AMQPConsumerBuilder.ExchangeType.DIRECT;

        AMQImpl.Queue.DeclareOk ok = mock(AMQImpl.Queue.DeclareOk.class);
        when(ok.getQueue()).thenReturn(MOCK_QUEUE);
        when(channel.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), anyMap())).thenReturn(ok);

        AMQPConsumerCallback consumerCallback = mock(AMQPConsumerCallback.class);

        AMQPCommonListenProperties commonListenProperties = AMQPSyncConsumerBuilder.builder()
                .callback(consumerCallback)
                .autoCreateAndBind(MOCK_EXCHANGE, exchangeType, MOCK_QUEUE, TEST_ROUTING_KEY)
                .poisonQueueEnabled(Boolean.TRUE)
                .prefetchCount(1)
                .buildListenProperties();

        amqpTransport.listenImpl(commonListenProperties);
        verify(amqpTransport).getConsumer(consumerCallback, commonListenProperties, "");
        verify(amqpTransport).autoCreateAndBind(MOCK_EXCHANGE, exchangeType.toString(), MOCK_QUEUE, TEST_ROUTING_KEY, true);
        verify(channel, never()).addShutdownListener(any(AMQPTransport.DynamicQueueCleanupShutdownListener.class));

        verify(channel, times(1)).exchangeDeclare(MOCK_EXCHANGE, exchangeType.toString(), true);
        verify(channel, times(1)).queueDeclare(MOCK_QUEUE, true, false, false, null);
        verify(channel, times(1)).queueBind(MOCK_QUEUE, MOCK_EXCHANGE, TEST_ROUTING_KEY);
        verify(channel, times(1)).queueDeclare(MOCK_QUEUE + AMQPTransport.POISON, true, false, false, null);
        verify(channel, times(1)).queueBind(MOCK_QUEUE + AMQPTransport.POISON, MOCK_EXCHANGE, TEST_ROUTING_KEY + AMQPTransport.POISON);

        verify(channel).basicConsume(eq(MOCK_QUEUE), eq(false), any(Consumer.class));
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
    public void testConnect_PrivateConnectionAndClosedChannel_ConnectsAndCreatesChannel() throws IOException, TimeoutException {
        amqpTransport = new AMQPTransport(false, "host", 1234, null);
        AMQPConnection connection = mock(AMQPConnection.class);
        amqpTransport.setConnection(connection);
        when(channel.isOpen()).thenReturn(false);
        amqpTransport.setChannel(channel);

        amqpTransport.connect(CONNECTION_PROPS);
        verify(connection).connect(CONNECTION_PROPS);
        verify(connection).createChannel();
    }

    @Test
    public void testConnect_SharedConnectionAndOpenChannel_DoesNothing() throws IOException, TimeoutException {
        AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.isConnected()).thenReturn(true);

        amqpTransport = spy(new AMQPTransport(connection));
        when(channel.isOpen()).thenReturn(true);
        amqpTransport.setChannel(channel);

        amqpTransport.connect(CONNECTION_PROPS);
        verify(connection, never()).connect(CONNECTION_PROPS);
        verify(connection, never()).createChannel();
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

    public static class DynamicQueueCleanupShutdownListenerTest {
        private static final String MOCK_DYNAMIC_QUEUE = "MOCK DYNAMIC QUEUE";
        private static final String MOCK_DYNAMIC_POISON_QUEUE = ".poison.MOCK DYNAMIC QUEUE";

        AMQPConnection connection;
        Channel mainChannel, cleanupChannel, poisonCleanupChannel;

        ArgumentCaptor<AMQPTransport.DynamicQueueCleanupShutdownListener> shutdownListenerCaptor;

        AMQPTransport.DynamicQueueCleanupShutdownListener shutdownListener;

        @Before
        public void setup() throws IOException {
            AMQP.Queue.DeclareOk ok = mock(AMQP.Queue.DeclareOk.class);
            when(ok.getQueue()).thenReturn("MOCK DYNAMIC QUEUE");

            connection = mock(AMQPConnection.class);
            mainChannel = mock(Channel.class);
            cleanupChannel = mock(Channel.class);
            poisonCleanupChannel = mock(Channel.class);

            when(connection.createChannel()).thenReturn(mainChannel);
            when(connection.isConnected()).thenReturn(true);
            when(mainChannel.isOpen()).thenReturn(true);
            when(mainChannel.queueDeclare()).thenReturn(ok);

            shutdownListenerCaptor = ArgumentCaptor.forClass(AMQPTransport.DynamicQueueCleanupShutdownListener.class);
        }

        private void listen(boolean poisonQueue) throws IOException {
            listen(poisonQueue, null);
        }

        private void listen(boolean poisonQueue, Class<? extends Throwable> expectedException) throws IOException {
            AMQPTransport transport = new AMQPTransport(connection);
            transport.connect(null);
            AMQPCommonListenProperties dynamicQueueListenProperties = AMQPSyncConsumerBuilder.builder()
                    .callback(mock(AMQPConsumerCallback.class))
                    .exchange("an exchange")
                    .dynamicQueueRoutingKey("a routing key")
                    .dynamicQueueCreation(true)
                    .poisonQueueEnabled(poisonQueue)
                    .prefetchCount(1)
                    .buildListenProperties();
            if (expectedException == null) {
                transport.listen(dynamicQueueListenProperties);
            }
            else {
                Assert.assertThrows(expectedException, ()->transport.listen(dynamicQueueListenProperties));
            }
            verify(mainChannel).addShutdownListener(shutdownListenerCaptor.capture());
            shutdownListener = shutdownListenerCaptor.getValue();
            when(connection.createChannel()).thenReturn(cleanupChannel, poisonCleanupChannel);

        }

        private void verifyNoQueuesAreDeleted() throws IOException {

            verify(cleanupChannel, never()).queueDelete(MOCK_DYNAMIC_QUEUE);
            verify(poisonCleanupChannel, never()).queueDelete(MOCK_DYNAMIC_POISON_QUEUE);
            verify(connection, times(1)).createChannel();
        }

        private void verifyOnlyDynamicQueueIsDeleted() throws IOException, TimeoutException {
            verify(cleanupChannel).queueDelete(MOCK_DYNAMIC_QUEUE);
            verify(cleanupChannel).close();
            verify(connection, times(2)).createChannel();
            verify(poisonCleanupChannel, never()).queueDelete(MOCK_DYNAMIC_POISON_QUEUE);
            verify(poisonCleanupChannel, never()).close();
        }

        private void verifyBothQueuesAreDeleted() throws IOException, TimeoutException {
            verify(cleanupChannel).queueDelete(MOCK_DYNAMIC_QUEUE);
            verify(cleanupChannel).close();
            verify(connection, times(3)).createChannel();
            verify(poisonCleanupChannel).queueDelete(MOCK_DYNAMIC_POISON_QUEUE);
            verify(poisonCleanupChannel).close();
        }

        @Test
        public void testShutdownCompleted_DynamicQueueCreatedWithoutPoisonQueue_AsynchronouslyDeletesDynamicQueue() throws IOException, TimeoutException {
            listen(false);
            shutdownListener.shutdownCompleted(null);
            shutdownListener.queueCleanupJob.join();
            verifyOnlyDynamicQueueIsDeleted();
        }

        @Test
        public void testShutdownCompleted_DynamicQueueCreatedWithPoisonQueue_AsynchronouslyDeletesBothQueues() throws IOException, TimeoutException {
            listen(true);
            shutdownListener.shutdownCompleted(null);
            shutdownListener.queueCleanupJob.join();
            verifyBothQueuesAreDeleted();
        }

        @Test
        public void testShutdownCompleted_ErrorOnDynamicQueueDeclare_DeletesNoQueues() throws IOException {
            when(mainChannel.queueDeclare()).thenThrow(new RuntimeException());
            listen(true, RuntimeException.class);
            shutdownListener.shutdownCompleted(null);
            shutdownListener.queueCleanupJob.join();
            verifyNoQueuesAreDeleted();
        }

        @Test
        public void testShutdownCompleted_ErrorOnDynamicQueueBind_DeletesDynamicQueue() throws IOException, TimeoutException {
            when(mainChannel.queueBind(eq(MOCK_DYNAMIC_QUEUE), anyString(), anyString())).thenThrow(new RuntimeException());
            listen(true, RuntimeException.class);
            shutdownListener.shutdownCompleted(null);
            shutdownListener.queueCleanupJob.join();
            verifyOnlyDynamicQueueIsDeleted();
        }

        @Test
        public void testShutdownCompleted_ErrorOnPoisonQueueDeclare_DeletesDynamicQueue() throws IOException, TimeoutException {

            when(mainChannel.queueDeclare(eq(MOCK_DYNAMIC_POISON_QUEUE), anyBoolean(), anyBoolean(), anyBoolean(), anyMap())).thenThrow(new RuntimeException());
            listen(true, RuntimeException.class);
            shutdownListener.shutdownCompleted(null);
            shutdownListener.queueCleanupJob.join();
            verifyOnlyDynamicQueueIsDeleted();
        }

        @Test
        public void testShutdownCompleted_ErrorOnPoisonQueueBind_DeletesBothQueues() throws IOException, TimeoutException {
            when(mainChannel.queueBind(eq(MOCK_DYNAMIC_POISON_QUEUE), anyString(), anyString())).thenThrow(new RuntimeException());
            listen(true, RuntimeException.class);
            shutdownListener.shutdownCompleted(null);
            shutdownListener.queueCleanupJob.join();
            verifyBothQueuesAreDeleted();
        }

        @Test
        public void testShutdownCompleted_ErrorOnDynamicQueueDelete_StillAttemptsToDeletePoisonQueue() throws IOException, TimeoutException {
            when(cleanupChannel.queueDelete(anyString())).thenThrow(new RuntimeException());
            listen(true);
            shutdownListener.shutdownCompleted(null);
            shutdownListener.queueCleanupJob.join();
            verifyBothQueuesAreDeleted();
        }

        @Test
        public void testShutdownCompleted_ErrorOnPoisonQueueDelete_StillClosesChannels() throws IOException, TimeoutException {
            when(poisonCleanupChannel.queueDelete(anyString())).thenThrow(new RuntimeException());
            listen(true);
            shutdownListener.shutdownCompleted(null);
            shutdownListener.queueCleanupJob.join();
            verifyBothQueuesAreDeleted();
        }
    }

}
