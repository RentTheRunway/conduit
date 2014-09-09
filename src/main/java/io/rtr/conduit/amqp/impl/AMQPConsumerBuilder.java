package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.consumer.ConsumerBuilder;
import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

public abstract class AMQPConsumerBuilder<T extends Transport
                                        , L extends TransportListenProperties
                                        , R extends AMQPConsumerBuilder>
                      extends ConsumerBuilder<T
                                            , AMQPConnectionProperties
                                            , L
                                            , AMQPListenContext> {
    private String username;
    private String password;
    private String exchange;
    private String queue;
    private String host = "localhost";
    private int port = 5672;
    private String virtualHost = "/";
    private int connectionTimeout = 10000; //! In milliseconds.
    private int heartbeatInterval = 60; //! In seconds.
    private int retryThreshold = 10;
    private boolean poisonQueueEnabled = true;
    private int prefetchCount = 0;
    private boolean purgeOnConnect;
    private boolean dynamicQueueCreation;
    private String poisonPrefix = "";
    private String dynamicQueueRoutingKey = "";

    protected AMQPConsumerBuilder() {
    }

    public static AMQPAsyncConsumerBuilder asynchronous() {
        return AMQPAsyncConsumerBuilder.builder();
    }

    public static AMQPSyncConsumerBuilder synchronous() {
        return AMQPSyncConsumerBuilder.builder();
    }

    private R builder() {
        return (R)this;
    }

    public R username(String username) {
        this.username = username;
        return builder();
    }

    public R purgeOnConnect(boolean purgeOnConnect) {
        this.purgeOnConnect = purgeOnConnect;
        return builder();
    }

    public R dynamicQueueCreation(boolean dynamicQueueCreation) {
        this.dynamicQueueCreation = dynamicQueueCreation;
        return builder();
    }

    public R prefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
        return builder();
    }

    public R password(String password) {
        this.password = password;
        return builder();
    }

    public R virtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return builder();
    }

    public R dynamicQueueRoutingKey(String dynamicQueueRoutingKey) {
        this.dynamicQueueRoutingKey = dynamicQueueRoutingKey;
        return builder();
    }

    public R connectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return builder();
    }

    public R heartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return builder();
    }

    public R exchange(String exchange) {
        this.exchange = exchange;
        return builder();
    }

    protected String getExchange() {
        return exchange;
    }

    public R queue(String queue) {
        this.queue = queue;
        return builder();
    }

    protected String getQueue() {
        return queue;
    }

    public R host(String host) {
        this.host = host;
        return builder();
    }

    protected String getHost() {
        return host;
    }

    public R port(int port) {
        this.port = port;
        return builder();
    }

    protected int getPort() {
        return port;
    }

    public R retryThreshold(int retryThreshold) {
        this.retryThreshold = retryThreshold;
        return builder();
    }

    protected int getRetryThreshold() {
        return retryThreshold;
    }

    public R poisonQueueEnabled(boolean enabled) {
    	this.poisonQueueEnabled = enabled;
    	return builder();
    }

    protected int getPrefetchCount() {
        return prefetchCount;
    }

    protected boolean isPurgeOnConnect() {
        return purgeOnConnect;
    }

    protected boolean isDynamicQueueCreation() {
        return dynamicQueueCreation;
    }

    protected String getPoisonPrefix() {
        return poisonPrefix;
    }

    protected String getDynamicQueueRoutingKey() {
        return dynamicQueueRoutingKey;
    }

    protected boolean isPoisonQueueEnabled() {
    	return poisonQueueEnabled;
    }
    
    @Override
    protected void validate() {
        assertNotNull(exchange, "exchange");
        assertNotNull(queue, "queue");
    }

    @Override
    protected AMQPConnectionProperties buildConnectionProperties() {
        return new AMQPConnectionProperties(username, password, virtualHost, connectionTimeout, heartbeatInterval);
    }
}
