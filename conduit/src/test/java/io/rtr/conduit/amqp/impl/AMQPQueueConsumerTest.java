package io.rtr.conduit.amqp.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.ActionResponse;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class AMQPQueueConsumerTest {
    @Test
    void testHandleDeliveryAcknowledge() {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        final AMQPConsumerCallback callback =
                new AMQPConsumerCallback() {
                    @Override
                    public ActionResponse handle(final AMQPMessageBundle messageBundle) {
                        messages.add(messageBundle);
                        return ActionResponse.acknowledge();
                    }

                    @Override
                    public void notifyOfActionFailure(final Exception e) {}

                    @Override
                    public void notifyOfShutdown(
                            final String consumerTag, final ShutdownSignalException sig) {}
                };

        final Channel channel = mock(Channel.class);
        final AMQPQueueConsumer consumer =
                spy(new AMQPQueueConsumer(channel, callback, 2, "", true));

        final String consumerTag = "foo";
        final Envelope envelope = new Envelope(0, false, "exchange", "key");
        final BasicProperties properties = new BasicProperties();

        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes(UTF_8));
        assertEquals(1, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
    }

    @Test
    void testHandleDeliveryRejectAndDiscard() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();
        final String actionReason =
                "Email was not sent since the user's email address was hard bounced by the Sailthru server";

        final AMQPConsumerCallback callback =
                new AMQPConsumerCallback() {
                    @Override
                    public ActionResponse handle(final AMQPMessageBundle messageBundle) {
                        messages.add(messageBundle);
                        return ActionResponse.discard(actionReason);
                    }

                    @Override
                    public void notifyOfActionFailure(final Exception e) {}

                    @Override
                    public void notifyOfShutdown(
                            final String consumerTag, final ShutdownSignalException sig) {}
                };

        final Channel channel = mock(Channel.class);
        final AMQPQueueConsumer consumer =
                spy(new AMQPQueueConsumer(channel, callback, 2, "", true));

        final String consumerTag = "foo";
        final Envelope envelope = new Envelope(0, false, "exchange", "key");
        final BasicProperties properties =
                new BasicProperties.Builder().headers(new HashMap<>()).build();
        final ArgumentCaptor<BasicProperties> captor =
                ArgumentCaptor.forClass(BasicProperties.class);

        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes(UTF_8));
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1))
                .basicPublish(
                        eq("exchange"), eq("key.poison"), captor.capture(), any(byte[].class));

        assertEquals(1, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals(
                actionReason,
                captor.getValue().getHeaders().get(ActionResponse.REASON_KEY).toString());
    }

    @Test
    void testHandleDeliveryRejectAndDiscardWithoutPoisonQueue() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        final AMQPConsumerCallback callback =
                new AMQPConsumerCallback() {
                    @Override
                    public ActionResponse handle(final AMQPMessageBundle messageBundle) {
                        messages.add(messageBundle);
                        return ActionResponse.discard();
                    }

                    @Override
                    public void notifyOfActionFailure(final Exception e) {}

                    @Override
                    public void notifyOfShutdown(
                            final String consumerTag, final ShutdownSignalException sig) {}
                };

        final Channel channel = mock(Channel.class);
        // disable poison queue
        final boolean poisonQueueEnabled = false;
        final AMQPQueueConsumer consumer =
                spy(new AMQPQueueConsumer(channel, callback, 2, "", poisonQueueEnabled));

        final String consumerTag = "foo";
        final Envelope envelope = new Envelope(0, false, "exchange", "key");
        final BasicProperties properties = new BasicProperties();

        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes(UTF_8));
        verify(channel, times(1)).basicReject(eq(0L), eq(false));

        // Should not publish to poison queue
        verify(channel, never())
                .basicPublish(
                        anyString(), anyString(), any(BasicProperties.class), any(byte[].class));

        assertEquals(1, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
    }

    @Test
    void testHandleDeliveryRejectAndRequeue() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        final AMQPConsumerCallback callback =
                new AMQPConsumerCallback() {
                    @Override
                    public ActionResponse handle(final AMQPMessageBundle messageBundle) {
                        messages.add(messageBundle);
                        return ActionResponse.retry();
                    }

                    @Override
                    public void notifyOfActionFailure(final Exception e) {}

                    @Override
                    public void notifyOfShutdown(
                            final String consumerTag, final ShutdownSignalException sig) {}
                };

        final Channel channel = mock(Channel.class);
        final AMQPQueueConsumer consumer =
                spy(new AMQPQueueConsumer(channel, callback, 2, "", true));

        final String consumerTag = "foo";
        final Envelope envelope = new Envelope(0, false, "exchange", "key");
        final BasicProperties properties =
                new BasicProperties().builder().headers(new HashMap<>()).build();

        final ArgumentCaptor<BasicProperties> captor =
                ArgumentCaptor.forClass(BasicProperties.class);

        // first time, we will retry
        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes(UTF_8));
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1))
                .basicPublish(eq("exchange"), eq("key"), captor.capture(), any(byte[].class));

        assertEquals(1, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals(1, captor.getValue().getHeaders().get("conduit-retry-count"));

        // second time, we will retry
        reset(channel);
        consumer.handleDelivery(consumerTag, envelope, captor.getValue(), "hello".getBytes(UTF_8));
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1))
                .basicPublish(eq("exchange"), eq("key"), captor.capture(), any(byte[].class));

        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(1).getBody()));
        assertEquals(2, captor.getValue().getHeaders().get("conduit-retry-count"));

        // third time, it goes to the poison queue
        reset(channel);
        consumer.handleDelivery(consumerTag, envelope, captor.getValue(), "hello".getBytes(UTF_8));
        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1))
                .basicPublish(
                        eq("exchange"), eq("key.poison"), captor.capture(), any(byte[].class));

        assertEquals(3, messages.size());
        assertEquals("hello", new String(messages.get(2).getBody()));
        assertEquals(2, captor.getValue().getHeaders().get("conduit-retry-count"));
    }
}
