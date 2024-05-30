package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.ActionResponse;
import io.rtr.conduit.amqp.AsyncResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class AMQPAsyncQueueConsumerTest {

    @Test
    void testRespondMultipleAcknowledgeAll() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        // this callback will acknowledge the 2nd message
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        messages.add(messageBundle);
                        if (++count > 1) {
                            response.respondMultiple(messageBundle, ActionResponse.acknowledge());
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 10, "", true));

        String consumerTag = "foo";
        Envelope envelope = new Envelope(0, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope, properties, "world".getBytes());
        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals("world", new String(messages.get(1).getBody()));
        verify(channel, times(1)).basicAck(anyLong(), eq(true));
    }

    @Test
    void testRespondSingleAcknowledge() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        // this callback will acknowledge the 2nd message
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        messages.add(messageBundle);
                        if (++count > 1) {
                            response.respondSingle(messageBundle, ActionResponse.acknowledge());
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 10, "", true));

        String consumerTag = "foo";
        Envelope envelope = new Envelope(0, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope, properties, "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope, properties, "world".getBytes());
        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals("world", new String(messages.get(1).getBody()));
        verify(channel, times(1)).basicAck(anyLong(), eq(false));
    }

    @Test
    void testRespondMultipleRejectAndDiscardAll() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        // this callback will discard the 2nd message
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        messages.add(messageBundle);

                        if (++count > 1) {
                            response.respondMultiple(messageBundle, ActionResponse.discard());
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 10, "", true));

        String consumerTag = "foo";
        Envelope envelope1 = new Envelope(0, false, "exchange", "key");
        Envelope envelope2 = new Envelope(1, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope1, properties, "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, properties, "world".getBytes());
        verify(channel, times(1)).basicNack(eq(1L), eq(true), eq(false));
        verify(channel, times(2))
                .basicPublish(
                        eq("exchange"),
                        eq("key.poison"),
                        any(AMQP.BasicProperties.class),
                        any(byte[].class));

        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals("world", new String(messages.get(1).getBody()));
    }

    @Test
    void testRespondMultipleRejectAndDiscardAllWithoutPoisonQueue() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        // this callback will discard the 2nd message
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        messages.add(messageBundle);

                        if (++count > 1) {
                            response.respondMultiple(messageBundle, ActionResponse.discard());
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        // disable poison queue
        boolean poisonQueueEnabled = false;
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 10, "", poisonQueueEnabled));

        String consumerTag = "foo";
        Envelope envelope1 = new Envelope(0, false, "exchange", "key");
        Envelope envelope2 = new Envelope(1, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope1, properties, "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, properties, "world".getBytes());
        verify(channel, times(1)).basicNack(eq(1L), eq(true), eq(false));
        verify(channel, never())
                .basicPublish(
                        anyString(),
                        anyString(),
                        any(AMQP.BasicProperties.class),
                        any(byte[].class));

        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals("world", new String(messages.get(1).getBody()));
    }

    @Test
    void testRespondSingleRejectAndDiscard() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        // this callback will discard only the 2nd message
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        messages.add(messageBundle);

                        if (++count > 1) {
                            response.respondSingle(messageBundle, ActionResponse.discard());
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 10, "", true));

        String consumerTag = "foo";
        Envelope envelope1 = new Envelope(0, false, "exchange", "key");
        Envelope envelope2 = new Envelope(1, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope1, properties, "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, properties, "world".getBytes());
        verify(channel, times(1)).basicReject(eq(1L), eq(false));
        verify(channel, times(1))
                .basicPublish(
                        eq("exchange"),
                        eq("key.poison"),
                        any(AMQP.BasicProperties.class),
                        any(byte[].class));

        // respond to left-over message
        reset(channel);
        consumer.respondSingle(
                new AMQPMessageBundle(consumerTag, envelope1, properties, "hello".getBytes()),
                ActionResponse.discard());

        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, times(1))
                .basicPublish(
                        eq("exchange"),
                        eq("key.poison"),
                        any(AMQP.BasicProperties.class),
                        any(byte[].class));

        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals("world", new String(messages.get(1).getBody()));
    }

    @Test
    void testRespondSingleRejectAndDiscardWithoutPoisonQueue() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        // this callback will discard only the 2nd message
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        messages.add(messageBundle);

                        if (++count > 1) {
                            response.respondSingle(messageBundle, ActionResponse.discard());
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        // disable poison queue
        boolean poisonQueueEnabled = false;
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 10, "", poisonQueueEnabled));

        String consumerTag = "foo";
        Envelope envelope1 = new Envelope(0, false, "exchange", "key");
        Envelope envelope2 = new Envelope(1, false, "exchange", "key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties();

        consumer.handleDelivery(consumerTag, envelope1, properties, "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, properties, "world".getBytes());
        verify(channel, times(1)).basicReject(eq(1L), eq(false));
        verify(channel, never())
                .basicPublish(
                        anyString(),
                        anyString(),
                        any(AMQP.BasicProperties.class),
                        any(byte[].class));

        // respond to left-over message
        reset(channel);
        consumer.respondSingle(
                new AMQPMessageBundle(consumerTag, envelope1, properties, "hello".getBytes()),
                ActionResponse.discard());

        verify(channel, times(1)).basicReject(eq(0L), eq(false));
        verify(channel, never())
                .basicPublish(
                        anyString(),
                        anyString(),
                        any(AMQP.BasicProperties.class),
                        any(byte[].class));

        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals("world", new String(messages.get(1).getBody()));
    }

    @Test
    void testRespondMultipleRejectAndRequeueAll() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        // this callback will requeue every 2nd message
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        messages.add(messageBundle);

                        if (++count > 1) {
                            response.respondMultiple(messageBundle, ActionResponse.retry());
                            count = 0;
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 2, "", true));

        String consumerTag = "foo";
        Envelope envelope1 = new Envelope(0, false, "exchange", "key");
        Envelope envelope2 = new Envelope(1, false, "exchange", "key");
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties().builder().headers(new HashMap<>()).build();

        ArgumentCaptor<AMQP.BasicProperties> captor =
                ArgumentCaptor.forClass(AMQP.BasicProperties.class);

        // first time, we will retry
        consumer.handleDelivery(consumerTag, envelope1, properties, "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, properties, "world".getBytes());
        verify(channel, times(1)).basicNack(eq(1L), eq(true), eq(false));
        verify(channel, times(2))
                .basicPublish(eq("exchange"), eq("key"), captor.capture(), any(byte[].class));

        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals("world", new String(messages.get(1).getBody()));
        assertEquals(1, captor.getValue().getHeaders().get("conduit-retry-count"));

        // second time, we will retry
        reset(channel);
        consumer.handleDelivery(consumerTag, envelope1, captor.getValue(), "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, captor.getValue(), "world".getBytes());
        verify(channel, times(1)).basicNack(eq(1L), eq(true), eq(false));
        verify(channel, times(2))
                .basicPublish(eq("exchange"), eq("key"), captor.capture(), any(byte[].class));

        assertEquals(4, messages.size());
        assertEquals("hello", new String(messages.get(2).getBody()));
        assertEquals("world", new String(messages.get(3).getBody()));
        assertEquals(2, captor.getValue().getHeaders().get("conduit-retry-count"));

        // third time, it goes to the poison queue
        reset(channel);
        consumer.handleDelivery(consumerTag, envelope1, captor.getValue(), "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, captor.getValue(), "world".getBytes());
        verify(channel, times(1)).basicNack(eq(1L), eq(true), eq(false));
        verify(channel, times(2))
                .basicPublish(
                        eq("exchange"), eq("key.poison"), captor.capture(), any(byte[].class));

        assertEquals(6, messages.size());
        assertEquals("hello", new String(messages.get(4).getBody()));
        assertEquals("world", new String(messages.get(5).getBody()));
        assertEquals(2, captor.getValue().getHeaders().get("conduit-retry-count"));
    }

    @Test
    void testRespondSingleRejectAndRequeue() throws Exception {
        final List<AMQPMessageBundle> messages = new ArrayList<>();

        // this callback will requeue every 2nd message
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        messages.add(messageBundle);

                        if (++count > 1) {
                            response.respondSingle(messageBundle, ActionResponse.retry());
                            count = 0;
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 2, "", true));

        String consumerTag = "foo";
        Envelope envelope1 = new Envelope(0, false, "exchange", "key");
        Envelope envelope2 = new Envelope(1, false, "exchange", "key");
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties().builder().headers(new HashMap<>()).build();

        ArgumentCaptor<AMQP.BasicProperties> captor =
                ArgumentCaptor.forClass(AMQP.BasicProperties.class);

        // first time, we will retry
        consumer.handleDelivery(consumerTag, envelope1, properties, "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, properties, "world".getBytes());
        verify(channel, times(1)).basicReject(eq(1L), eq(false));
        verify(channel, times(1))
                .basicPublish(eq("exchange"), eq("key"), captor.capture(), any(byte[].class));

        assertEquals(2, messages.size());
        assertEquals("hello", new String(messages.get(0).getBody()));
        assertEquals("world", new String(messages.get(1).getBody()));
        assertEquals(1, captor.getValue().getHeaders().get("conduit-retry-count"));

        // second time, we will retry
        reset(channel);
        consumer.handleDelivery(consumerTag, envelope1, captor.getValue(), "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, captor.getValue(), "world".getBytes());
        verify(channel, times(1)).basicReject(eq(1L), eq(false));
        verify(channel, times(1))
                .basicPublish(eq("exchange"), eq("key"), captor.capture(), any(byte[].class));

        assertEquals(4, messages.size());
        assertEquals("hello", new String(messages.get(2).getBody()));
        assertEquals("world", new String(messages.get(3).getBody()));
        assertEquals(2, captor.getValue().getHeaders().get("conduit-retry-count"));

        // third time, it goes to the poison queue
        reset(channel);
        consumer.handleDelivery(consumerTag, envelope1, captor.getValue(), "hello".getBytes());
        consumer.handleDelivery(consumerTag, envelope2, captor.getValue(), "world".getBytes());
        verify(channel, times(1)).basicReject(eq(1L), eq(false));
        verify(channel, times(1))
                .basicPublish(
                        eq("exchange"), eq("key.poison"), captor.capture(), any(byte[].class));

        assertEquals(6, messages.size());
        assertEquals("hello", new String(messages.get(4).getBody()));
        assertEquals("world", new String(messages.get(5).getBody()));
        assertEquals(2, captor.getValue().getHeaders().get("conduit-retry-count"));
    }

    @Test
    void testHandleDeliveryMixedResponses() throws Exception {
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        switch (count++) {
                            case 0:
                                response.respondMultiple(
                                        messageBundle, ActionResponse.acknowledge());
                                break;

                            case 1:
                                response.respondMultiple(messageBundle, ActionResponse.discard());
                                break;

                            case 2:
                                response.respondMultiple(messageBundle, ActionResponse.retry());
                                break;

                            case 3:
                                // skip, because we'll discard 3 and 4 in step 4
                                break;

                            case 4:
                                response.respondMultiple(messageBundle, ActionResponse.discard());
                                break;
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 0, "", true));

        String consumerTag = "foo";
        Envelope envelope1 = new Envelope(0, false, "exchange", "key");
        Envelope envelope2 = new Envelope(1, false, "exchange", "key");
        Envelope envelope3 = new Envelope(2, false, "exchange", "key");
        Envelope envelope4 = new Envelope(3, false, "exchange", "key");
        Envelope envelope5 = new Envelope(4, false, "exchange", "key");
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties().builder().headers(new HashMap<>()).build();

        byte[] ack = "ack".getBytes();
        byte[] discard = "discard".getBytes();
        byte[] requeue = "requeue".getBytes();

        consumer.handleDelivery(consumerTag, envelope1, properties, ack);
        consumer.handleDelivery(consumerTag, envelope2, properties, discard);
        consumer.handleDelivery(consumerTag, envelope3, properties, requeue);
        consumer.handleDelivery(consumerTag, envelope4, properties, discard);
        consumer.handleDelivery(consumerTag, envelope5, properties, discard);

        // first message
        verify(channel, times(1)).basicAck(eq(0L), eq(true));
        // last four messages we use 3 nacks: discard, requeue, discardx2
        verify(channel, times(3)).basicNack(anyLong(), eq(true), eq(false));

        // 1 message rejected and sent to poison
        // 1 message over retry count of 0 and sent to poison
        // 2 rejected and sent to poison
        verify(channel, times(4))
                .basicPublish(
                        eq("exchange"),
                        eq("key.poison"),
                        any(AMQP.BasicProperties.class),
                        any(byte[].class));
    }

    @Test
    void testHandleDeliveryMixedRetryValues() throws Exception {
        AMQPAsyncConsumerCallback callback =
                new AMQPAsyncConsumerCallback() {

                    private int count = 0;

                    // after 6 messages, reject and requeue
                    @Override
                    public void handle(AMQPMessageBundle messageBundle, AsyncResponse response) {
                        if (++count > 5) {
                            response.respondMultiple(messageBundle, ActionResponse.retry());
                        }
                    }

                    @Override
                    public void notifyOfActionFailure(Exception e) {}

                    @Override
                    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {}
                };

        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 1, "", true));
        final Map<Long, String> routingKeys = new HashMap<>();
        final List<Long> deliveryTags = new ArrayList<>();

        doAnswer(
                        invocationOnMock -> {
                            Long deliveryTag =
                                    Long.parseLong(
                                            new String(
                                                    (byte[]) invocationOnMock.getArguments()[3]));
                            String routingKey = invocationOnMock.getArguments()[1].toString();
                            routingKeys.put(deliveryTag, routingKey);
                            deliveryTags.add(deliveryTag);
                            return null;
                        })
                .when(channel)
                .basicPublish(
                        anyString(),
                        anyString(),
                        any(AMQP.BasicProperties.class),
                        any(byte[].class));

        for (int i = 0; i < 6; i++) {
            Envelope envelope = new Envelope(i, false, "exchange", "key");
            HashMap<String, Object> headers = new HashMap<>();

            // conduit-retry-count values:
            // - 0 1 2 0 1
            if (i > 0) {
                headers.put("conduit-retry-count", (i - 1) % 3);
            }

            AMQP.BasicProperties properties =
                    new AMQP.BasicProperties().builder().headers(headers).build();

            consumer.handleDelivery(
                    "foo", envelope, properties, Integer.valueOf(i).toString().getBytes());
        }

        // 3 of our messages should have been sent to poison queue, while 3 should have been retried
        Map<Long, String> expectedRoutingKeys = new HashMap<>();
        expectedRoutingKeys.put(0L, "key");
        expectedRoutingKeys.put(1L, "key");
        expectedRoutingKeys.put(2L, "key.poison");
        expectedRoutingKeys.put(3L, "key.poison");
        expectedRoutingKeys.put(4L, "key");
        expectedRoutingKeys.put(5L, "key.poison");

        // First the retried ones call publish, then the poison ones after that
        List<Long> expectedDeliveryTags = new ArrayList<>();
        expectedDeliveryTags.add(0L);
        expectedDeliveryTags.add(1L);
        expectedDeliveryTags.add(4L);
        expectedDeliveryTags.add(2L);
        expectedDeliveryTags.add(3L);
        expectedDeliveryTags.add(5L);

        assertEquals(expectedRoutingKeys, routingKeys);
        assertEquals(expectedDeliveryTags, deliveryTags);
    }

    @Test
    void testShutdownHandlerInvocation() {
        AMQPAsyncConsumerCallback callback = mock(AMQPAsyncConsumerCallback.class);
        Channel channel = mock(Channel.class);
        AMQPAsyncQueueConsumer consumer =
                spy(new AMQPAsyncQueueConsumer(channel, callback, 1, "", true));
        consumer.handleShutdownSignal("foo", mock(ShutdownSignalException.class));
        verify(callback, times(1)).notifyOfShutdown(eq("foo"), any(ShutdownSignalException.class));
    }
}
