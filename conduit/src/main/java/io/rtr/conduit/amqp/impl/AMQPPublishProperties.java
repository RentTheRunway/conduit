package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.transport.TransportPublishProperties;

public class AMQPPublishProperties implements TransportPublishProperties {
    private String exchange;
    private String routingKey;
    private long timeout;
    private boolean confirmEnabled;

    AMQPPublishProperties(String exchange, String routingKey, long timeout, boolean confirmEnabled) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.timeout = timeout;
        this.confirmEnabled = confirmEnabled;
    }

    public AMQPPublishProperties(String exchange, String routingKey) {
        this(exchange, routingKey, 100, false);
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isConfirmEnabled() {
        return confirmEnabled;
    }
}
