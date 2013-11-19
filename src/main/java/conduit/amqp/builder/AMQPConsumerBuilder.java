package conduit.amqp.builder;

import conduit.amqp.AMQPConnectionProperties;
import conduit.amqp.AMQPListenContext;
import conduit.consumer.ConsumerBuilder;
import conduit.transport.Transport;
import conduit.transport.TransportListenProperties;

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
    private int connectionTimeout = 0; //! 0 is infinite.
    private int heartbeatInterval = 60; //! In seconds.
    private int retryThreshold = 10;

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

    public R password(String password) {
        this.password = password;
        return builder();
    }

    public R virtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
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
