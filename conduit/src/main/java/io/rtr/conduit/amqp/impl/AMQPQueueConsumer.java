package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.ActionResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AMQPQueueConsumer extends DefaultConsumer {
    private static final Logger log = LoggerFactory.getLogger(AMQPQueueConsumer.class);
    private static final String HEADER_RETRY_COUNT = "conduit-retry-count";
    private AMQPConsumerCallback callback;
    private int threshold;
    protected final Channel channel;
    private String poisonPrefix;
    private boolean poisonQueueEnabled;

    AMQPQueueConsumer(
            Channel channel,
            AMQPConsumerCallback callback,
            int threshold,
            String poisonPrefix,
            boolean poisonQueueEnabled) {
        super(channel);
        this.callback = callback;
        this.threshold = threshold;
        this.channel = channel;
        this.poisonPrefix = poisonPrefix;
        this.poisonQueueEnabled = poisonQueueEnabled;
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        log.info("Shutdown handler invoked");
        callback.notifyOfShutdown(consumerTag, sig);
    }

    @Override
    public void handleDelivery(
            String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        ActionResponse actionResponse;
        AMQPMessageBundle messageBundle =
                new AMQPMessageBundle(consumerTag, envelope, properties, body);

        try {
            actionResponse = callback.handle(messageBundle);
        } catch (RuntimeException e) {
            // ! Blanket exception handler for notifying, via the log, that the user-supplied
            //  callback let an exception propagate. In such cases, all the listeners are stopped.
            log.error("The user-supplied callback allowed an exception to propagate.");
            log.error("Catastrophic - all listeners have stopped! Exception: ", e);
            throw e;
        }

        respond(messageBundle, actionResponse);
    }

    private void respond(AMQPMessageBundle messageBundle, ActionResponse actionResponse) {
        // ! We can't issue any blocking amqp calls in the context of this method, otherwise
        //  channel's internal thread(s) will deadlock. Both, basicAck and basicReject are
        //  asynchronous.
        Envelope envelope = messageBundle.getEnvelope();
        Long deliveryTag = envelope.getDeliveryTag();
        byte[] body = messageBundle.getBody();
        AMQP.BasicProperties properties = messageBundle.getBasicProperties();

        try {
            switch (actionResponse.getAction()) {
                case Acknowledge:
                    ack(deliveryTag);
                    break;

                case RejectAndDiscard:
                    log.warn("Discarding message, body = " + new String(body));
                    // ! Let the broker know we rejected this message. Then publish this bad boy
                    // onto
                    //  the poison queue. Don't bother confirming for two reasons; we don't care,
                    // and
                    //  we can't issue blocking calls here.
                    publishToPoisonQueue(envelope, properties, actionResponse.getReason(), body);
                    reject(deliveryTag);
                    break;

                case RejectAndRequeue:
                    log.warn("Received an unknown message, body = " + new String(body));
                    log.warn("\tAdjusting headers for retry.");

                    if (!retry(envelope, properties, body)) {
                        publishToPoisonQueue(
                                envelope, properties, actionResponse.getReason(), body);
                    }
                    reject(deliveryTag);
                    break;
            }
        } catch (Exception e) {
            callback.notifyOfActionFailure(e);
        }
    }

    private void ack(Long deliveryTag) throws IOException {
        channel.basicAck(deliveryTag, false);
    }

    private void reject(long deliveryTag) throws IOException {
        channel.basicReject(deliveryTag, false);
    }

    protected boolean retry(Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        Map<String, Object> headers = properties.getHeaders();
        Object retryHeader = headers.get(HEADER_RETRY_COUNT);
        int retryCount = 0;

        if (retryHeader != null) {
            try {
                retryCount = Integer.parseInt(retryHeader.toString());
            } catch (NumberFormatException ignored) {
                log.error(
                        "Received an invalid retry-count header, body = "
                                + new String(body)
                                + ", header = "
                                + retryHeader);
            }
        } else {
            log.warn("Received message without retry-count header, body = " + new String(body));
        }

        if (retryCount >= threshold) {
            return false;
        }

        headers = new HashMap<String, Object>(headers);
        headers.put(HEADER_RETRY_COUNT, retryCount + 1);
        AMQP.BasicProperties retryProperties =
                new AMQP.BasicProperties()
                        .builder()
                        .type(properties.getType())
                        .deliveryMode(properties.getDeliveryMode())
                        .priority(properties.getPriority())
                        .headers(headers)
                        .build();

        channel.basicPublish(
                envelope.getExchange(), envelope.getRoutingKey(), retryProperties, body);

        return true;
    }

    protected void publishToPoisonQueue(
            Envelope envelope, AMQP.BasicProperties properties, String reason, byte[] body)
            throws IOException {
        if (!poisonQueueEnabled) {
            return;
        }

        if (reason != null && !reason.trim().isEmpty()) {
            Map<String, Object> headers = new HashMap<String, Object>(properties.getHeaders());
            headers.put(ActionResponse.REASON_KEY, reason);
            properties = createCopyWithNewHeaders(properties, headers);
        }

        channel.basicPublish(
                envelope.getExchange(),
                envelope.getRoutingKey() + poisonPrefix + ".poison",
                properties,
                body);
    }

    protected AMQP.BasicProperties createCopyWithNewHeaders(
            AMQP.BasicProperties properties, Map<String, Object> headers) {
        return new AMQP.BasicProperties()
                .builder()
                .contentType(properties.getContentType())
                .contentEncoding(properties.getContentEncoding())
                .headers(headers)
                .deliveryMode(properties.getDeliveryMode())
                .priority(properties.getPriority())
                .correlationId(properties.getCorrelationId())
                .replyTo(properties.getReplyTo())
                .expiration(properties.getExpiration())
                .messageId(properties.getMessageId())
                .timestamp(properties.getTimestamp())
                .type(properties.getType())
                .userId(properties.getUserId())
                .appId(properties.getAppId())
                .clusterId(properties.getClusterId())
                .build();
    }
}
