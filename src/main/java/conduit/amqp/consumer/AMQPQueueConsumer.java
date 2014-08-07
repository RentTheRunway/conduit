package conduit.amqp.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import conduit.amqp.AMQPConsumerCallback;
import conduit.amqp.AMQPMessageBundle;
import conduit.amqp.Action;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AMQPQueueConsumer extends DefaultConsumer {
    private static final Logger log = Logger.getLogger(AMQPQueueConsumer.class);
    private static final String HEADER_RETRY_COUNT = "conduit-retry-count";
    private AMQPConsumerCallback callback;
    private int threshold;
    protected final Channel channel;
    private String poisonPrefix;
    private boolean poisonQueueEnabled;

    public AMQPQueueConsumer(Channel channel, AMQPConsumerCallback callback, int threshold, String poisonPrefix, boolean poisonQueueEnabled) {
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
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        Action action;
        AMQPMessageBundle messageBundle = new AMQPMessageBundle(
                consumerTag
              , envelope
              , properties
              , body
        );

        try {
            action = callback.handle(messageBundle);
        } catch (RuntimeException e) {
            //! Blanket exception handler for notifying, via the log, that the user-supplied
            //  callback let an exception propagate. In such cases, all the listeners are stopped.
            log.error("The user-supplied callback allowed an exception to propagate.");
            log.error("Catastrophic - all listeners have stopped! Exception: ", e);
            throw e;
        }

        respond(messageBundle, action);
    }

    private void respond(AMQPMessageBundle messageBundle, Action action) {
        //! We can't issue any blocking amqp calls in the context of this method, otherwise
        //  channel's internal thread(s) will deadlock. Both, basicAck and basicReject are
        //  asynchronous.
        Envelope envelope = messageBundle.getEnvelope();
        Long deliveryTag = envelope.getDeliveryTag();
        byte[] body = messageBundle.getBody();
        AMQP.BasicProperties properties = messageBundle.getBasicProperties();

        try {
            switch (action) {
                case Acknowledge:
                    ack(deliveryTag);
                    break;

                case RejectAndDiscard:
                    log.warn("Discarding message, body = " + new String(body));
                    //! Let the broker know we rejected this message. Then publish this bad boy onto
                    //  the poison queue. Don't bother confirming for two reasons; we don't care, and
                    //  we can't issue blocking calls here.
                    publishToPoisonQueue(envelope, properties, body);
                    reject(deliveryTag);
                    break;

                case RejectAndRequeue:
                    log.warn("Received an unknown message, body = " + new String(body));
                    log.warn("\tAdjusting headers for retry.");

                    if (!retry(envelope, properties, body)) {
                        publishToPoisonQueue(envelope, properties, body);
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

    protected boolean retry(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        Map<String, Object> headers = properties.getHeaders();
        Object retryHeader = headers.get(HEADER_RETRY_COUNT);
        int retryCount = 0;

        if (retryHeader != null) {
            try {
                retryCount = Integer.parseInt(retryHeader.toString());
            } catch (NumberFormatException ignored) {
                log.error("Received an invalid retry-count header, body = "
                        + new String(body)
                        + ", header = " + retryHeader);
            }
        } else {
            log.warn("Received message without retry-count header, body = " + new String(body));
        }

        if (retryCount >= threshold) {
            return false;
        }

        headers = new HashMap<String, Object>(headers);
        headers.put(HEADER_RETRY_COUNT, retryCount + 1);
        AMQP.BasicProperties retryProperties = new AMQP.BasicProperties()
                .builder()
                .type(properties.getType())
                .deliveryMode(properties.getDeliveryMode())
                .priority(properties.getPriority())
                .headers(headers)
                .build();

        channel.basicPublish(
                envelope.getExchange()
              , envelope.getRoutingKey()
              , retryProperties
              , body);

        return true;
    }

    protected void publishToPoisonQueue(Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
    	if (!poisonQueueEnabled) {
    		return;
    	}
    	
        channel.basicPublish(
                envelope.getExchange()
              , envelope.getRoutingKey() + poisonPrefix + ".poison"
              , properties
              , body
        );
    }
}
