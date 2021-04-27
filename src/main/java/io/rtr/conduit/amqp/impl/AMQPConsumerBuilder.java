package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.consumer.ConsumerBuilder;
import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

public abstract class AMQPConsumerBuilder<T extends Transport
        , L extends TransportListenProperties
        , R extends AMQPConsumerBuilder<?,?,?>>
        extends ConsumerBuilder<T
        , AMQPConnectionProperties
        , L
        , AMQPListenContext> {
    private String username;
    private String password;
    private String exchange;
    private String queue;
    private boolean isAutoDeleteQueue = false;
    private boolean ssl;
    private String host = "localhost";
    private int port = 5672;
    private AMQPConnection sharedConnection;
    private String virtualHost = "/";
    private int connectionTimeout = 10000; //! In milliseconds.
    private int heartbeatInterval = 60; //! In seconds.
    private boolean automaticRecoveryEnabled = true;
    private int retryThreshold = 10;
    private boolean poisonQueueEnabled = true;
    private int prefetchCount = 1;
    private boolean purgeOnConnect;
    private boolean dynamicQueueCreation;
    private String poisonPrefix = "";
    private String dynamicQueueRoutingKey = "";
    private String routingKey = "";
    private boolean autoCreateAndBind = false;
    private ExchangeType exchangeType = ExchangeType.DIRECT;

    protected AMQPConsumerBuilder() {
    }

    public static AMQPAsyncConsumerBuilder asynchronous() {
        return AMQPAsyncConsumerBuilder.builder();
    }

    public static AMQPSyncConsumerBuilder synchronous() {
        return AMQPSyncConsumerBuilder.builder();
    }

    private R builder() {
        return (R) this;
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

    public R automaticRecoveryEnabled(boolean automaticRecoveryEnabled) {
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
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

    public R isAutoDeleteQueue(boolean isAutoDeleteQueue) {
        this.isAutoDeleteQueue = isAutoDeleteQueue;
        return builder();
    }

    protected boolean isAutoDeleteQueue() {
        return isAutoDeleteQueue;
    }


    public R ssl(boolean ssl) {
        this.ssl = ssl;
        return builder();
    }

    protected boolean isSsl() {
        return ssl;
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

    public R sharedConnection(AMQPConnection connection) {
        sharedConnection = connection;
        return builder();
    }

    public AMQPConnection getSharedConnection() {
        return sharedConnection;
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

    /**
     * Auto create the exchange, queue and then bind them together.
     *
     * There are a few assumptions to keep this 'light' and compatible with typical usage:
     *  - Queues are NOT exclusive to this connection
     *  - No custom arguments
     *  - No dead letter routing
     *  - No custom TTL
     *
     * By default, queue will be durable (NOT auto-delete)
     */
    public R autoCreateAndBind(String exchange, ExchangeType exchangeType, String queue, String routingKey) {
        this.autoCreateAndBind = true;
        this.exchange = exchange;
        this.queue = queue;
        this.isAutoDeleteQueue = isAutoDeleteQueue();
        this.exchangeType = exchangeType;
        this.routingKey = (routingKey == null) ? "" : routingKey;
        return builder();
    }

    public boolean isAutoCreateAndBind() {
        return autoCreateAndBind;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public String getExchangeType() {
        return exchangeType.toString();
    }

    protected int getPrefetchCount() {
        return prefetchCount;
    }

    protected boolean shouldPurgeOnConnect() {
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
        if (dynamicQueueCreation && autoCreateAndBind) {
            throw new IllegalArgumentException("Both dynamicQueueCreation and autoCreateAndBind are enabled.");
        }
        if (!dynamicQueueCreation) {
            assertNotNull(queue, "queue");
        } else {
            assertNotNull(dynamicQueueRoutingKey, "dynamicQueueRoutingKey");
        }
        if (autoCreateAndBind) {
            assertNotNull(queue, "queue");
            assertNotNull(exchangeType, "exchangeType");
            assertNotNull(routingKey, "routingKey");
            if (exchangeType == ExchangeType.FANOUT && isPoisonQueueEnabled()) {
                throw new IllegalArgumentException("Fanout exchanges do not support poison queues");
            }
        }
        if (sharedConnection != null && (username != null || password != null || !virtualHost.equals("/"))) {
            throw new IllegalArgumentException(
                    String.format("Username ('%s'), password ('%s') or virtualHost ('%s') should not be specified for a consumer if using a shared connection, it only needs these if using it's own private connection.", username, password, virtualHost)
            );
        }

    }

    @Override
    protected AMQPConnectionProperties buildConnectionProperties() {
        return new AMQPConnectionProperties(username, password, virtualHost, connectionTimeout,
                heartbeatInterval, automaticRecoveryEnabled);
    }

    public enum ExchangeType {
        DIRECT("direct"),
        FANOUT("fanout"),
        TOPIC("topic"),
        CONSISTENT_HASH("x-consistent-hash"); // This kind of exchange can only be created if plugin "consistent-hash-exchange" is enabled

        private final String name;

        ExchangeType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }

}
