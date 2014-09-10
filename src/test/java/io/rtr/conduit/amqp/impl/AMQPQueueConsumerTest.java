package io.rtr.conduit.amqp.impl;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.Action;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class AMQPQueueConsumerTest {
    @Test
    public void testHandleDeliveryAcknowledge() {
        final List<AMQPMessageBundle> messages = new ArrayList<AMQPMessageBundle>();

        AMQPConsumerCallback callback = new AMQPConsumerCallback() {
            @Override
            public Action handle(AMQPMessageBundle messageBundle) {
                messages.add(messageBundle);
                return Action.Acknowledge;
            }

            @Override
            public void notifyOfActionFailure(Exception e) {
            }

            @Override
            public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {
            }
        };

        Channel channel = mock(Channel.class);
        AMQPQueueConsumer consumer = spy(new AMQPQueueConsumer(channel, callback, 2, "", true));

        String consumerTag = "foo";
        Envelope envelope = new Envelope(0, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes());
        assertEquals(1, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
    }

    @Test
    public void testHandleDeliveryRejectAndDiscard() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<AMQPMessageBundle>();

        AMQPConsumerCallback callback = new AMQPConsumerCallback() {
            @Override
            public Action handle(AMQPMessageBundle messageBundle) {
                messages.add(messageBundle);
                return Action.RejectAndDiscard;
            }

            @Override
            public void notifyOfActionFailure(Exception e) {
            }

            @Override
            public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {
            }
        };

        Channel channel = mock(Channel.class);
        AMQPQueueConsumer consumer = spy(new AMQPQueueConsumer(channel, callback, 2, "", true));

        String consumerTag = "foo";
        Envelope envelope = new Envelope(0, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes());
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1)).basicPublish(eq("exchange")
                                             , eq("key.poison")
                                             , any(AMQP.BasicProperties.class)
                                             , any(byte[].class));

        assertEquals(1, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
    }
    
    @Test
    public void testHandleDeliveryRejectAndDiscardWithoutPoisonQueue() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<AMQPMessageBundle>();

        AMQPConsumerCallback callback = new AMQPConsumerCallback() {
            @Override
            public Action handle(AMQPMessageBundle messageBundle) {
                messages.add(messageBundle);
                return Action.RejectAndDiscard;
            }

            @Override
            public void notifyOfActionFailure(Exception e) {
            }

            @Override
            public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {
            }
        };

        Channel channel = mock(Channel.class);
        //disable poison queue
        boolean poisonQueueEnabled = false;
        AMQPQueueConsumer consumer = spy(new AMQPQueueConsumer(channel, callback, 2, "", poisonQueueEnabled));

        String consumerTag = "foo";
        Envelope envelope = new Envelope(0, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes());
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        
        //Should not publish to poison queue
        verify(channel, never()).basicPublish(anyString()
                                             , anyString()
                                             , any(AMQP.BasicProperties.class)
                                             , any(byte[].class));

        assertEquals(1, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
    }

    @Test
    public void testHandleDeliveryRejectAndRequeue() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<AMQPMessageBundle>();

        AMQPConsumerCallback callback = new AMQPConsumerCallback() {
            @Override
            public Action handle(AMQPMessageBundle messageBundle) {
                messages.add(messageBundle);
                return Action.RejectAndRequeue;
            }

            @Override
            public void notifyOfActionFailure(Exception e) {
            }

            @Override
            public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {
            }
        };

        Channel channel = mock(Channel.class);
        AMQPQueueConsumer consumer = spy(new AMQPQueueConsumer(channel, callback, 2, "", true));

        String consumerTag = "foo";
        Envelope envelope = new Envelope(0, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties()
                                                    .builder()
                                                    .headers(new HashMap<String, Object>())
                                                    .build();

        ArgumentCaptor<AMQP.BasicProperties> captor = ArgumentCaptor.forClass(AMQP.BasicProperties.class);

        // first time, we will retry
        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes());
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1)).basicPublish(eq("exchange"), eq("key"), captor.capture(), any(byte[].class));

        assertEquals(1, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals(1, captor.getValue().getHeaders().get("conduit-retry-count"));

        // second time, we will retry
        reset(channel);
        consumer.handleDelivery(consumerTag, envelope, captor.getValue(), "hello".getBytes());
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1)).basicPublish(eq("exchange"), eq("key"), captor.capture(), any(byte[].class));

        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(1).getBody()));
        assertEquals(2, captor.getValue().getHeaders().get("conduit-retry-count"));

        // third time, it goes to the poison queue
        reset(channel);
        consumer.handleDelivery(consumerTag, envelope, captor.getValue(), "hello".getBytes());
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1)).basicPublish(eq("exchange"), eq("key.poison"), captor.capture(), any(byte[].class));

        assertEquals(3, messages.size());
        assertEquals("hello", new String(messages.get(2).getBody()));
        assertEquals(2, captor.getValue().getHeaders().get("conduit-retry-count"));
    }
}
