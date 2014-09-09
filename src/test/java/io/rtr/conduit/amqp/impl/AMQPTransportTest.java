package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.impl.AMQImpl;
import io.rtr.conduit.amqp.AMQPConsumerCallback;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AMQPTransportTest {
    @Test
    public void testListenImplDynamicQueues() throws IOException {
        AMQPTransport amqpTransport = spy(new AMQPTransport("host", 1234));
        Channel channel = mock(Channel.class);

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
        AMQPTransport amqpTransport = spy(new AMQPTransport("host", 1234));
        Channel channel = mock(Channel.class);

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
}
