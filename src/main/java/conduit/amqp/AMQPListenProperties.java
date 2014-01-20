package conduit.amqp;

import conduit.transport.TransportListenProperties;

/**
 * There is a single restriction on the exchange/queue architecture; the consumer
 * queue must have a respective poison counterpart. Given a queue "Q", there must
 * exist a queue "Q.poison".
 */
public class AMQPListenProperties implements TransportListenProperties {
    private AMQPConsumerCallback callback;
    private String exchange;
    private String queue;
    private int threshold;
    private boolean isDrainOnListen;

    public static class AMQPListenPropertiesBuilder{
        private AMQPConsumerCallback callback;
        private String exchange;
        private String queue;
        private int threshold;
        private boolean isDrainOnListen;

        public AMQPListenPropertiesBuilder(AMQPConsumerCallback callback, String exchange, String queue){
            this.callback = callback;
            this.exchange = exchange;
            this.queue = queue;
        }

        public AMQPListenPropertiesBuilder setThreshold(int threshold) {
            this.threshold = threshold;
            return this;
        }

        public AMQPListenPropertiesBuilder setDrainOnListen(boolean isDrainOnListen) {
            this.isDrainOnListen = isDrainOnListen;
            return this;
        }

        public AMQPListenProperties create(){
            AMQPListenProperties amqpListenProperties = new AMQPListenProperties(callback, exchange, queue);
            amqpListenProperties.threshold = threshold;
            amqpListenProperties.isDrainOnListen = isDrainOnListen;
            return amqpListenProperties;
        }
    }

    public AMQPListenProperties(
            AMQPConsumerCallback callback
          , String exchange
          , String queue
    ) {
        this(callback, exchange, queue, 10);
    }

    public AMQPListenProperties(
            AMQPConsumerCallback callback
          , String exchange
          , String queue
          , int threshold
    ) {
        this.callback = callback;
        this.exchange = exchange;
        this.queue = queue;
        this.threshold = threshold;
    }

    public AMQPConsumerCallback getCallback() {
        return callback;
    }

    public String getExchange() {
        return exchange;
    }

    public String getQueue() {
        return queue;
    }

    public int getThreshold() {
        return threshold;
    }

    public boolean isDrainOnListen() {
        return isDrainOnListen;
    }
}
