package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.ActionResponse;
import io.rtr.conduit.amqp.AsyncResponse;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class AMQPAsyncQueueConsumer extends AMQPQueueConsumer implements AsyncResponse {
    private static final Logger log = Logger.getLogger(AMQPQueueConsumer.class);
    private final AMQPAsyncConsumerCallback callback;
    private final Map<Long, AMQPMessageBundle> unacknowledgedMessages = new LinkedHashMap<Long, AMQPMessageBundle>();

    AMQPAsyncQueueConsumer(Channel channel, AMQPAsyncConsumerCallback callback, int threshold, String poisonPrefix, boolean poisonQueueEnabled) {
        super(channel, null, threshold, poisonPrefix, poisonQueueEnabled);
        this.callback = callback;
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        log.info("Shutdown handler invoked");
        callback.notifyOfShutdown(consumerTag, sig);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        AMQPMessageBundle messageBundle = new AMQPMessageBundle(
                consumerTag
              , envelope
              , properties
              , body
        );

        try {
            unacknowledgedMessages.put(messageBundle.getEnvelope().getDeliveryTag(), messageBundle);
            callback.handle(messageBundle, this);
        } catch (RuntimeException e) {
            //! Blanket exception handler for notifying, via the log, that the user-supplied
            //  callback let an exception propagate. In such cases, all the listeners are stopped.
            log.error("The user-supplied callback allowed an exception to propagate.");
            log.error("Catastrophic - all listeners have stopped! Exception: ", e);
            throw e;
        }
    }

    protected void respond(AMQPMessageBundle messageBundle, ActionResponse actionResponse, boolean multiple) {
        //! We can't issue any blocking amqp calls in the context of this method, otherwise
        //  channel's internal thread(s) will deadlock. Both, basicAck and basicReject are
        //  asynchronous.
        Envelope envelope = messageBundle.getEnvelope();
        Long deliveryTag = envelope.getDeliveryTag();
        byte[] body = messageBundle.getBody();
        AMQP.BasicProperties properties = messageBundle.getBasicProperties();
        if(actionResponse.getReason()!=null && !actionResponse.getReason().trim().isEmpty()) {
            Map<String, Object> headers = new HashMap<String, Object>(properties.getHeaders());
            headers.put(ActionResponse.REASON_KEY, actionResponse.getReason());
            properties = createCopyWithNewHeaders(properties, headers);
        }

        try {
            switch (actionResponse.getAction()) {
                case Acknowledge:
                    ack(deliveryTag, multiple);
                    break;

                case RejectAndDiscard:
                    log.warn("Discarding message, body = " + new String(body));
                    //! Let the broker know we rejected this message. Then publish this bad boy onto
                    //  the poison queue. Don't bother confirming for two reasons; we don't care, and
                    //  we can't issue blocking calls here.
                    publishToPoisonQueue(envelope, properties, body, multiple);
                    reject(deliveryTag, multiple);
                    break;

                case RejectAndRequeue:
                    log.warn("Received an unknown message, body = " + new String(body));
                    log.warn("\tAdjusting headers for retry.");

                    if (!retry(envelope, properties, body, multiple)) {
                        publishToPoisonQueue(envelope, properties, body, multiple);
                    }
                    reject(deliveryTag, multiple);
                    break;
            }
        } catch (Exception e) {
            callback.notifyOfActionFailure(e);
        }
    }

    private void removeUnacknowledgedMessages(Long deliveryTag, boolean multiple) {
        if (!multiple) {
            unacknowledgedMessages.remove(deliveryTag);
            return;
        }

        Iterator<Entry<Long, AMQPMessageBundle>> messages = unacknowledgedMessages.entrySet().iterator();

        while (messages.hasNext()) {
            Entry<Long, AMQPMessageBundle> next = messages.next();

            if (next.getKey() > deliveryTag) {
                break;
            }
            messages.remove();
        }
    }

    private void ack(Long deliveryTag, boolean multiple) throws IOException {
        removeUnacknowledgedMessages(deliveryTag, multiple);
        channel.basicAck(deliveryTag, multiple);
    }

    private void reject(long deliveryTag, boolean multiple) throws IOException {
        removeUnacknowledgedMessages(deliveryTag, multiple);
        if (multiple) {
            channel.basicNack(deliveryTag, true, false);
        } else {
            channel.basicReject(deliveryTag, false);
        }
    }

    /**
     * Send all unacknowledged messages to poison queue, up-to and including this one
     * @throws IOException
     */
    private void publishToPoisonQueue(Envelope envelope
                                    , AMQP.BasicProperties properties
                                    , byte[] body
                                    , boolean multiple) throws IOException {
        if (!multiple) {
            super.publishToPoisonQueue(envelope, properties, body);
            return;
        }

        Iterator<AMQPMessageBundle> messages = unacknowledgedMessages.values().iterator();
        long deliveryTag = envelope.getDeliveryTag();

        while (messages.hasNext()) {
            AMQPMessageBundle next = messages.next();

            if (next.getEnvelope().getDeliveryTag() > deliveryTag) {
                break;
            }

            super.publishToPoisonQueue(next.getEnvelope(), next.getBasicProperties(), next.getBody());
        }
    }

    /**
     * Retry all previously unacknowledged messages, up-to and including the one given
     * @throws IOException
     */
    private boolean retry(Envelope envelope, AMQP.BasicProperties properties, byte[] body, boolean multiple)
            throws IOException {
        boolean retry = true;

        if (!multiple) {
            retry = super.retry(envelope, properties, body);
            if (retry) {
                unacknowledgedMessages.remove(envelope.getDeliveryTag());
            }
            return retry;
        }

        Iterator<AMQPMessageBundle> messages = unacknowledgedMessages.values().iterator();
        long deliveryTag = envelope.getDeliveryTag();

        while (messages.hasNext()) {
            AMQPMessageBundle next = messages.next();

            if (next.getEnvelope().getDeliveryTag() > deliveryTag) {
                break;
            }

            boolean retried = super.retry(next.getEnvelope(), next.getBasicProperties(), next.getBody());
            retry &= retried;
            if (retried) {
                // we have successfully sent a retry message, remove from queue so later, after we return,
                // publishToPoisonQueue() won't send this message to the poison queue
                messages.remove();
            }
        }

        return retry;
    }

    @Override
    public void respondMultiple(AMQPMessageBundle messageBundle, ActionResponse actionResponse) {
        respond(messageBundle, actionResponse, true);
    }

    @Override
    public void respondSingle(AMQPMessageBundle messageBundle, ActionResponse actionResponse) {
        respond(messageBundle, actionResponse, false);
    }
}
