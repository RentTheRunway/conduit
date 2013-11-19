package conduit.amqp.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import conduit.amqp.AMQPAsyncConsumerCallback;
import conduit.amqp.AMQPMessageBundle;
import conduit.amqp.Action;
import conduit.amqp.AsyncResponse;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class AMQPAsyncQueueConsumer extends AMQPQueueConsumer implements AsyncResponse {
    private static final Logger log = Logger.getLogger(AMQPQueueConsumer.class);
    private final AMQPAsyncConsumerCallback callback;
    private final Queue<AMQPMessageBundle> unacknowledgedMessages = new LinkedList<AMQPMessageBundle>();

    public AMQPAsyncQueueConsumer(Channel channel, AMQPAsyncConsumerCallback callback, int threshold) {
        super(channel, null, threshold);
        this.callback = callback;
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
            unacknowledgedMessages.offer(messageBundle);
            callback.handle(messageBundle, this);
        } catch (RuntimeException e) {
            //! Blanket exception handler for notifying, via the log, that the user-supplied
            //  callback let an exception propagate. In such cases, all the listeners are stopped.
            log.error("The user-supplied callback allowed an exception to propagate.");
            log.error("Catastrophic - all listeners have stopped! Exception: ", e);
            throw e;
        }
    }

    private void trimUnacknowlegedMessages(Long deliveryTag) {
        Iterator<AMQPMessageBundle> messages = unacknowledgedMessages.iterator();

        while (messages.hasNext()) {
            AMQPMessageBundle next = messages.next();

            if (next.getEnvelope().getDeliveryTag() > deliveryTag) {
                break;
            }
            messages.remove();
        }
    }

    @Override
    protected void ack(Long deliveryTag) throws IOException {
        trimUnacknowlegedMessages(deliveryTag);
        channel.basicAck(deliveryTag, true);
    }

    @Override
    protected void reject(long deliveryTag) throws IOException {
        trimUnacknowlegedMessages(deliveryTag);
        channel.basicNack(deliveryTag, true, false);
    }

    /**
     * Send all unacknowledged messages to poison queue, up-to and including this one
     * @throws IOException
     */
    @Override
    protected void publishToPoisonQueue(Envelope envelope
                                      , AMQP.BasicProperties properties
                                      , byte[] body) throws IOException {
        Iterator<AMQPMessageBundle> messages = unacknowledgedMessages.iterator();
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
    @Override
    protected boolean retry(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        Iterator<AMQPMessageBundle> messages = unacknowledgedMessages.iterator();
        boolean retry = true;
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
    public void respondMultiple(AMQPMessageBundle messageBundle, Action action) {
        respond(messageBundle, action);
    }
}
