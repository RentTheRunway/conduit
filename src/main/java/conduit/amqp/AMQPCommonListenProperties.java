package conduit.amqp;

public class AMQPCommonListenProperties {
    private String exchange;
    private String queue;
    private int threshold;
    private int prefetchCount;
    private boolean poisonQueueEnabled;
    private boolean purgeOnConnect;
    private boolean dynamicQueueCreation;
    private String poisonPrefix;
    private String dynamicQueueRoutingKey;

    private AMQPCommonListenProperties(String exchange, String queue, int threshold, int prefetchCount, boolean poisonQueueEnabled, boolean purgeOnConnect, boolean dynamicQueueCreation, String poisonPrefix, String dynamicQueueRoutingKey) {
        this.exchange = exchange;
        this.queue = queue;
        this.threshold = threshold;
        this.prefetchCount = prefetchCount;
        this.poisonQueueEnabled = poisonQueueEnabled;
        this.purgeOnConnect = purgeOnConnect;
        this.dynamicQueueCreation = dynamicQueueCreation;
        this.poisonPrefix = poisonPrefix;
        this.dynamicQueueRoutingKey = dynamicQueueRoutingKey;
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

    public int getPrefetchCount() {
        return prefetchCount;
    }

    public boolean isPoisonQueueEnabled() {
        return poisonQueueEnabled;
    }

    public boolean isPurgeOnConnect() {
        return purgeOnConnect;
    }

    public boolean isDynamicQueueCreation() {
        return dynamicQueueCreation;
    }

    public String getPoisonPrefix() {
        return poisonPrefix;
    }

    public String getDynamicQueueRoutingKey() {
        return dynamicQueueRoutingKey;
    }

    public static class AMQPCommonListenPropertiesBuilder {
        private String exchange;
        private String queue;
        private int threshold;
        private int prefetchCount;
        private boolean poisonQueueEnabled;
        private boolean purgeOnConnect;
        private boolean dynamicQueueCreation;
        private String poisonPrefix;
        private String dynamicQueueRoutingKey;

        public AMQPCommonListenPropertiesBuilder setExchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public AMQPCommonListenPropertiesBuilder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public AMQPCommonListenPropertiesBuilder setThreshold(int threshold) {
            this.threshold = threshold;
            return this;
        }

        public AMQPCommonListenPropertiesBuilder setPrefetchCount(int prefetchCount) {
            this.prefetchCount = prefetchCount;
            return this;
        }

        public AMQPCommonListenPropertiesBuilder setPoisonQueueEnabled(boolean poisonQueueEnabled) {
            this.poisonQueueEnabled = poisonQueueEnabled;
            return this;
        }

        public AMQPCommonListenPropertiesBuilder setPurgeOnConnect(boolean purgeOnConnect) {
            this.purgeOnConnect = purgeOnConnect;
            return this;
        }

        public AMQPCommonListenPropertiesBuilder setDynamicQueueCreation(boolean dynamicQueueCreation) {
            this.dynamicQueueCreation = dynamicQueueCreation;
            return this;
        }

        public AMQPCommonListenPropertiesBuilder setPoisonPrefix(String poisonPrefix) {
            this.poisonPrefix = poisonPrefix;
            return this;
        }

        public AMQPCommonListenPropertiesBuilder setDynamicQueueRoutingKey(String dynamicQueueRoutingKey) {
            this.dynamicQueueRoutingKey = dynamicQueueRoutingKey;
            return this;
        }

        public AMQPCommonListenProperties createAMQPCommonListenProperties() {
            if(dynamicQueueCreation && queue != null){
                throw new IllegalArgumentException("queue must be null when using dynamic queue creation");
            }

            if(prefetchCount == 0){
                prefetchCount = 1;
            }
            return new AMQPCommonListenProperties(exchange, queue, threshold, prefetchCount, poisonQueueEnabled, purgeOnConnect, dynamicQueueCreation, poisonPrefix, dynamicQueueRoutingKey);
        }
    }
}
