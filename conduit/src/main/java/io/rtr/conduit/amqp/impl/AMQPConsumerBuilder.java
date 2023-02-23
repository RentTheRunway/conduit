package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.consumer.ConsumerBuilder;
import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

public abstract class AMQPConsumerBuilder<T extends Transport, L extends TransportListenProperties, R extends AMQPConsumerBuilder<?,?,?>>
        extends ConsumerBuilder<T, AMQPConnectionProperties, L, AMQPListenContext> {
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
    private boolean exclusive = false;
    private long networkRecoveryInterval = 5000L;
    private Long topologyRecoveryInterval;
    private Integer topologyRecoveryMaxAttempts = Integer.MAX_VALUE;

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

    public R username(final String username) {
        this.username = username;
        return builder();
    }

    public String getUsername() {
        return username;
    }

    public R purgeOnConnect(final boolean purgeOnConnect) {
        this.purgeOnConnect = purgeOnConnect;
        return builder();
    }

    public boolean shouldPurgeOnConnect() {
        return purgeOnConnect;
    }

    public R dynamicQueueCreation(final boolean dynamicQueueCreation) {
        this.dynamicQueueCreation = dynamicQueueCreation;
        return builder();
    }

    protected boolean isDynamicQueueCreation() {
        return dynamicQueueCreation;
    }

    public R poisonPrefix(final String poisonPrefix) {
        this.poisonPrefix = poisonPrefix;
        return builder();
    }

    public String getPoisonPrefix() {
        return poisonPrefix;
    }

    public R prefetchCount(final int prefetchCount) {
        this.prefetchCount = prefetchCount;
        return builder();
    }

    protected int getPrefetchCount() {
        return prefetchCount;
    }

    public R password(final String password) {
        this.password = password;
        return builder();
    }

    public String getPassword() {
        return password;
    }

    public R virtualHost(final String virtualHost) {
        this.virtualHost = virtualHost;
        return builder();
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public R dynamicQueueRoutingKey(final String dynamicQueueRoutingKey) {
        this.dynamicQueueRoutingKey = dynamicQueueRoutingKey;
        return builder();
    }

    protected String getDynamicQueueRoutingKey() {
        return dynamicQueueRoutingKey;
    }

    public R connectionTimeout(final int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return builder();
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public R heartbeatInterval(final int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return builder();
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public R automaticRecoveryEnabled(final boolean automaticRecoveryEnabled) {
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
        return builder();
    }

    public boolean isAutomaticRecoveryEnabled() {
        return automaticRecoveryEnabled;
    }

    public R networkRecoveryInterval(final long networkRecoveryInterval) {
        this.networkRecoveryInterval = networkRecoveryInterval;
        return builder();
    }

    public long getNetworkRecoveryInterval() {
        return networkRecoveryInterval;
    }

    public R topologyRecoveryInterval(final Long topologyRecoveryInterval) {
        this.topologyRecoveryInterval = topologyRecoveryInterval;
        return builder();
    }

    public Long getTopologyRecoveryInterval() {
        return topologyRecoveryInterval;
    }

    public R topologyRecoveryMaxAttempts(final Integer topologyRecoveryMaxAttempts) {
        this.topologyRecoveryMaxAttempts = topologyRecoveryMaxAttempts;
        return builder();
    }

    public Integer getTopologyRecoveryMaxAttempts() {
        return topologyRecoveryMaxAttempts;
    }

    public R exchange(final String exchange) {
        this.exchange = exchange;
        return builder();
    }

    protected String getExchange() {
        return exchange;
    }

    public R queue(final String queue) {
        this.queue = queue;
        return builder();
    }

    protected String getQueue() {
        return queue;
    }

    public R autoDeleteQueue(final boolean autoDeleteQueue) {
        isAutoDeleteQueue = autoDeleteQueue;
        return builder();
    }

    protected boolean isAutoDeleteQueue() {
        return isAutoDeleteQueue;
    }

    public R ssl(final boolean ssl) {
        this.ssl = ssl;
        return builder();
    }

    protected boolean isSsl() {
        return ssl;
    }

    public R host(final String host) {
        this.host = host;
        return builder();
    }

    protected String getHost() {
        return host;
    }

    public R port(final int port) {
        this.port = port;
        return builder();
    }

    protected int getPort() {
        return port;
    }

    public R sharedConnection(final AMQPConnection connection) {
        sharedConnection = connection;
        return builder();
    }

    public AMQPConnection getSharedConnection() {
        return sharedConnection;
    }

    public R exclusive(final boolean exclusive) {
        this.exclusive = exclusive;
        return builder();
    }

    protected boolean getExclusive() { return exclusive; }

    public R retryThreshold(final int retryThreshold) {
        this.retryThreshold = retryThreshold;
        return builder();
    }

    protected int getRetryThreshold() {
        return retryThreshold;
    }

    public R poisonQueueEnabled(final boolean enabled) {
        this.poisonQueueEnabled = enabled;
        return builder();
    }

    protected boolean isPoisonQueueEnabled() {
        return poisonQueueEnabled;
    }

    public R routingKey(final String routingKey) {
        this.routingKey = routingKey;
        return builder();
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public R autoCreateAndBind(final boolean autoCreateAndBind) {
        this.autoCreateAndBind = autoCreateAndBind;
        return builder();
    }

    public boolean isAutoCreateAndBind() {
        return autoCreateAndBind;
    }

    public R exchangeType(final ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
        return builder();
    }

    public String getExchangeType() {
        return exchangeType.toString();
    }

    /**
     * Auto create the exchange, queue and then bind them together.
     *<p>
     * There are a few assumptions to keep this 'light' and compatible with typical usage:
     *  - Queues are NOT exclusive to this connection
     *  - No custom arguments
     *  - No dead letter routing
     *  - No custom TTL
     *<p>
     * By default, queue will be durable (NOT auto-delete)
     */
    public R autoCreateAndBind(final String exchange,
                               final ExchangeType exchangeType,
                               final String queue,
                               final String routingKey) {
        this.autoCreateAndBind = true;
        this.exchange = exchange;
        this.queue = queue;
        this.exchangeType = exchangeType;
        this.routingKey = (routingKey == null) ? "" : routingKey;
        return builder();
    }

    public R autoCreateAndBind(final String exchange,
                               final ExchangeType exchangeType,
                               final String queue,
                               final boolean isAutoDeleteQueue,
                               final String routingKey) {
        this.autoCreateAndBind = true;
        this.exchange = exchange;
        this.queue = queue;
        this.isAutoDeleteQueue = isAutoDeleteQueue;
        this.exchangeType = exchangeType;
        this.routingKey = (routingKey == null) ? "" : routingKey;
        return builder();
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
                    String.format("Username ('%s'), password ('%s') or virtualHost ('%s') should not be specified for a consumer if using a shared connection, it only needs these if using it's own private connection.",
                            username, password, virtualHost)
            );
        }
    }

    @Override
    protected AMQPConnectionProperties buildConnectionProperties() {
        return new AMQPConnectionProperties(username, password, virtualHost, connectionTimeout,
                heartbeatInterval, automaticRecoveryEnabled, networkRecoveryInterval,
                topologyRecoveryInterval, topologyRecoveryMaxAttempts);
    }

    public enum ExchangeType {
        DIRECT("direct"),
        FANOUT("fanout"),
        TOPIC("topic"),
        CONSISTENT_HASH("x-consistent-hash"); // This kind of exchange can only be created if plugin "consistent-hash-exchange" is enabled

        private final String name;

        ExchangeType(final String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
