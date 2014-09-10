package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.transport.TransportListenProperties;

public abstract class AMQPCommonListenProperties implements TransportListenProperties {
    private String exchange;
    private String queue;
    private int threshold;
    private int prefetchCount;
    private boolean poisonQueueEnabled;
    private boolean purgeOnConnect;
    private boolean dynamicQueueCreation;
    private String poisonPrefix;
    private String dynamicQueueRoutingKey;

    AMQPCommonListenProperties(String exchange, String queue, int threshold, int prefetchCount, boolean poisonQueueEnabled, boolean purgeOnConnect, boolean dynamicQueueCreation, String poisonPrefix, String dynamicQueueRoutingKey) {
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

    public boolean shouldPurgeOnConnect() {
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

}
