package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.transport.TransportListenProperties;

public abstract class AMQPCommonListenProperties implements TransportListenProperties {
    private String exchange;
    private String queue;
    private boolean isAutoDeleteQueue;
    private int threshold;
    private int prefetchCount;
    private boolean poisonQueueEnabled;
    private boolean purgeOnConnect;
    private boolean dynamicQueueCreation;
    private String poisonPrefix;
    private String dynamicQueueRoutingKey;
    private String routingKey;
    private boolean autoCreateAndBind;
    private String exchangeType;
    private boolean exclusive;

    AMQPCommonListenProperties(
            String exchange,
            String queue,
            boolean isAutoDeleteQueue,
            int threshold,
            int prefetchCount,
            boolean poisonQueueEnabled,
            boolean purgeOnConnect,
            boolean dynamicQueueCreation,
            String poisonPrefix,
            String dynamicQueueRoutingKey,
            boolean autoCreateAndBind,
            String exchangeType,
            String routingKey,
            boolean exclusive) {
        this.exchange = exchange;
        this.queue = queue;
        this.isAutoDeleteQueue = isAutoDeleteQueue;
        this.threshold = threshold;
        this.prefetchCount = prefetchCount;
        this.poisonQueueEnabled = poisonQueueEnabled;
        this.purgeOnConnect = purgeOnConnect;
        this.dynamicQueueCreation = dynamicQueueCreation;
        this.poisonPrefix = poisonPrefix;
        this.dynamicQueueRoutingKey = dynamicQueueRoutingKey;
        this.routingKey = routingKey;
        this.autoCreateAndBind = autoCreateAndBind;
        this.exchangeType = exchangeType;
        this.exclusive = exclusive;
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

    public String getRoutingKey() {
        return routingKey;
    }

    public String getExchangeType() {
        return exchangeType;
    }

    public boolean isAutoCreateAndBind() {
        return autoCreateAndBind;
    }

    public boolean isAutoDeleteQueue() {
        return isAutoDeleteQueue;
    }

    public boolean getExclusive() {
        return exclusive;
    }
}
