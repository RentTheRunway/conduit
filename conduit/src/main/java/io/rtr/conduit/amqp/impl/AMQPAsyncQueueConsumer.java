package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.ActionResponse;
import io.rtr.conduit.amqp.AsyncResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class AMQPAsyncQueueConsumer extends AMQPQueueConsumer implements AsyncResponse {
    private static final Logger log = LoggerFactory.getLogger(AMQPAsyncQueueConsumer.class);
    private final AMQPAsyncConsumerCallback callback;
    private final Map<Long, AMQPMessageBundle> unacknowledgedMessages =
            new LinkedHashMap<Long, AMQPMessageBundle>();

    AMQPAsyncQueueConsumer(
            final Channel channel,
            final AMQPAsyncConsumerCallback callback,
            final int threshold,
            final String poisonPrefix,
            final boolean poisonQueueEnabled) {
        super(channel, null, threshold, poisonPrefix, poisonQueueEnabled);
        this.callback = callback;
    }

    @Override
    public void handleShutdownSignal(final String consumerTag, final ShutdownSignalException sig) {
        log.info("Shutdown handler invoked");
        callback.notifyOfShutdown(consumerTag, sig);
    }

    @Override
    public void handleDelivery(
            final String consumerTag,
            final Envelope envelope,
            final AMQP.BasicProperties properties,
            final byte[] body) {
        final AMQPMessageBundle messageBundle =
                new AMQPMessageBundle(consumerTag, envelope, properties, body);

        try {
            unacknowledgedMessages.put(messageBundle.getEnvelope().getDeliveryTag(), messageBundle);
            callback.handle(messageBundle, this);
        } catch (final RuntimeException e) {
            // ! Blanket exception handler for notifying, via the log, that the user-supplied
            //  callback let an exception propagate. In such cases, all the listeners are stopped.
            log.error("The user-supplied callback allowed an exception to propagate.");
            log.error("Catastrophic - all listeners have stopped! Exception: ", e);
            throw e;
        }
    }

    protected void respond(
            final AMQPMessageBundle messageBundle,
            final ActionResponse actionResponse,
            final boolean multiple) {
        // ! We can't issue any blocking amqp calls in the context of this method, otherwise
        //  channel's internal thread(s) will deadlock. Both, basicAck and basicReject are
        //  asynchronous.
        final Envelope envelope = messageBundle.getEnvelope();
        final Long deliveryTag = envelope.getDeliveryTag();
        final byte[] body = messageBundle.getBody();
        final AMQP.BasicProperties properties = messageBundle.getBasicProperties();

        try {
            switch (actionResponse.getAction()) {
                case Acknowledge:
                    this.ack(deliveryTag, multiple);
                    break;

                case RejectAndDiscard:
                    log.warn("Discarding message, body = " + new String(body));
                    // ! Let the broker know we rejected this message. Then publish this bad boy
                    // onto
                    //  the poison queue. Don't bother confirming for two reasons; we don't care,
                    // and
                    //  we can't issue blocking calls here.
                    this.publishToPoisonQueue(
                            envelope, properties, actionResponse.getReason(), body, multiple);
                    this.reject(deliveryTag, multiple);
                    break;

                case RejectAndRequeue:
                    log.warn("Received an unknown message, body = " + new String(body));
                    log.warn("\tAdjusting headers for retry.");

                    if (!this.retry(envelope, properties, body, multiple)) {
                        this.publishToPoisonQueue(
                                envelope, properties, actionResponse.getReason(), body, multiple);
                    }
                    this.reject(deliveryTag, multiple);
                    break;
            }
        } catch (final Exception e) {
            callback.notifyOfActionFailure(e);
        }
    }

    private void removeUnacknowledgedMessages(final Long deliveryTag, final boolean multiple) {
        if (!multiple) {
            unacknowledgedMessages.remove(deliveryTag);
            return;
        }

        final Iterator<Entry<Long, AMQPMessageBundle>> messages =
                unacknowledgedMessages.entrySet().iterator();

        while (messages.hasNext()) {
            final Entry<Long, AMQPMessageBundle> next = messages.next();

            if (next.getKey() > deliveryTag) {
                break;
            }
            messages.remove();
        }
    }

    private void ack(final Long deliveryTag, final boolean multiple) throws IOException {
        this.removeUnacknowledgedMessages(deliveryTag, multiple);
        channel.basicAck(deliveryTag, multiple);
    }

    private void reject(final long deliveryTag, final boolean multiple) throws IOException {
        this.removeUnacknowledgedMessages(deliveryTag, multiple);
        if (multiple) {
            channel.basicNack(deliveryTag, true, false);
        } else {
            channel.basicReject(deliveryTag, false);
        }
    }

    /**
     * Send all unacknowledged messages to poison queue, up-to and including this one
     *
     * @throws IOException
     */
    private void publishToPoisonQueue(
            final Envelope envelope,
            final AMQP.BasicProperties properties,
            final String reason,
            final byte[] body,
            final boolean multiple)
            throws IOException {
        if (!multiple) {
            super.publishToPoisonQueue(envelope, properties, reason, body);
            return;
        }

        final Iterator<AMQPMessageBundle> messages = unacknowledgedMessages.values().iterator();
        final long deliveryTag = envelope.getDeliveryTag();

        while (messages.hasNext()) {
            final AMQPMessageBundle next = messages.next();

            if (next.getEnvelope().getDeliveryTag() > deliveryTag) {
                break;
            }

            super.publishToPoisonQueue(
                    next.getEnvelope(), next.getBasicProperties(), reason, next.getBody());
        }
    }

    /**
     * Retry all previously unacknowledged messages, up-to and including the one given
     *
     * @throws IOException
     */
    private boolean retry(
            final Envelope envelope,
            final AMQP.BasicProperties properties,
            final byte[] body,
            final boolean multiple)
            throws IOException {
        boolean retry = true;

        if (!multiple) {
            retry = super.retry(envelope, properties, body);
            if (retry) {
                unacknowledgedMessages.remove(envelope.getDeliveryTag());
            }
            return retry;
        }

        final Iterator<AMQPMessageBundle> messages = unacknowledgedMessages.values().iterator();
        final long deliveryTag = envelope.getDeliveryTag();

        while (messages.hasNext()) {
            final AMQPMessageBundle next = messages.next();

            if (next.getEnvelope().getDeliveryTag() > deliveryTag) {
                break;
            }

            final boolean retried =
                    super.retry(next.getEnvelope(), next.getBasicProperties(), next.getBody());
            retry &= retried;
            if (retried) {
                // we have successfully sent a retry message, remove from queue so later, after we
                // return,
                // publishToPoisonQueue() won't send this message to the poison queue
                messages.remove();
            }
        }

        return retry;
    }

    @Override
    public void respondMultiple(
            final AMQPMessageBundle messageBundle, final ActionResponse actionResponse) {
        this.respond(messageBundle, actionResponse, true);
    }

    @Override
    public void respondSingle(
            final AMQPMessageBundle messageBundle, final ActionResponse actionResponse) {
        this.respond(messageBundle, actionResponse, false);
    }
}
