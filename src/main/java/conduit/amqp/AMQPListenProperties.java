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
    private boolean purgeOnConnect;
    private String poisonPrefix;
    private boolean poisonQueueEnabled;

    public static class AMQPListenPropertiesBuilder{
        private AMQPConsumerCallback callback;
        private String exchange;
        private String queue;
        private int threshold;
        private boolean purgeOnConnect;
        private String poisonPrefix;
        private boolean poisonQueueEnabled = true;

        public AMQPListenPropertiesBuilder(AMQPConsumerCallback callback, String exchange, String queue){
            this.callback = callback;
            this.exchange = exchange;
            this.queue = queue;
        }

        public AMQPListenPropertiesBuilder setPoisonPrefix(String poisonPrefix) {
            this.poisonPrefix = poisonPrefix;
            return this;
        }
        
        public AMQPListenPropertiesBuilder setPoisonQueueEnabled(boolean enabled) {
        	this.poisonQueueEnabled = enabled;
        	return this;
        }

        public AMQPListenPropertiesBuilder setThreshold(int threshold) {
            this.threshold = threshold;
            return this;
        }

        public AMQPListenPropertiesBuilder setPurgeOnConnect(boolean purgeOnConnect) {
            this.purgeOnConnect = purgeOnConnect;
            return this;
        }

        public AMQPListenProperties create(){
            AMQPListenProperties amqpListenProperties = new AMQPListenProperties(callback, exchange, queue);
            amqpListenProperties.threshold = threshold;
            amqpListenProperties.purgeOnConnect = purgeOnConnect;
            if(poisonPrefix != null){
                amqpListenProperties.poisonPrefix = poisonPrefix;
            }
            amqpListenProperties.poisonQueueEnabled = poisonQueueEnabled;
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
        this(callback, exchange, queue, threshold, true);
    }
    
    public AMQPListenProperties(
            AMQPConsumerCallback callback
          , String exchange
          , String queue
          , int threshold
          , boolean poisonQueueEnabled
    ) {
        this.callback = callback;
        this.exchange = exchange;
        this.queue = queue;
        this.threshold = threshold;
        this.poisonPrefix = "";
        this.poisonQueueEnabled = poisonQueueEnabled;
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

    public boolean isPurgeOnConnect() {
        return purgeOnConnect;
    }

    public String getPoisonPrefix() {
        return poisonPrefix;
    }
    
    public boolean isPoisonQueueEnabled() {
    	return poisonQueueEnabled;
    }
}
