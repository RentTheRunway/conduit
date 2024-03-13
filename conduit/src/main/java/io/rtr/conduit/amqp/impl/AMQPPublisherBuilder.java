package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.MetricsCollector;
import com.rabbitmq.client.impl.MicrometerMetricsCollector;
import io.rtr.conduit.amqp.publisher.PublisherBuilder;

public class AMQPPublisherBuilder extends PublisherBuilder<AMQPTransport
                                                         , AMQPConnectionProperties
                                                         , AMQPPublishProperties
                                                         , AMQPPublishContext> {
    protected String username;
    protected String password;
    protected String exchange;
    protected String routingKey;
    protected boolean ssl;
    protected String host = "localhost";
    protected String virtualHost = "/";
    protected int port = 5672;
    private AMQPConnection sharedConnection;
    protected int publishTimeout = 100;
    protected int connectionTimeout = 10000; //! In milliseconds
    protected int heartbeatInterval = 60; //! In seconds
    protected boolean automaticRecoveryEnabled = true;
    protected boolean confirmEnabled = false;
    protected MetricsCollector metricsCollector;

    protected AMQPPublisherBuilder() {
    }

    public static AMQPPublisherBuilder builder() {
        return new AMQPPublisherBuilder();
    }

    public AMQPPublisherBuilder username(String username) {
        this.username = username;
        return this;
    }

    public AMQPPublisherBuilder password(String password) {
        this.password = password;
        return this;
    }

    public AMQPPublisherBuilder virtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return this;
    }

    public AMQPPublisherBuilder ssl(boolean ssl) {
        this.ssl = ssl;
        return this;
    }

    public AMQPPublisherBuilder host(String host) {
        this.host = host;
        return this;
    }

    public AMQPPublisherBuilder port(int port) {
        this.port = port;
        return this;
    }

    public AMQPPublisherBuilder sharedConnection(AMQPConnection connection) {
        sharedConnection = connection;
        return this;
    }

    public AMQPConnection getSharedConnection() {
        return sharedConnection;
    }

    public AMQPPublisherBuilder publishTimeout(int timeout) {
        this.publishTimeout = timeout;
        return this;
    }

    public AMQPPublisherBuilder connectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public AMQPPublisherBuilder heartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public AMQPPublisherBuilder automaticRecoveryEnabled(boolean automaticRecoveryEnabled) {
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
        return this;
    }

    public AMQPPublisherBuilder exchange(String exchange) {
        this.exchange = exchange;
        return this;
    }

    public AMQPPublisherBuilder routingKey(String routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    public AMQPPublisherBuilder confirmEnabled(boolean confirmEnabled) {
        this.confirmEnabled = confirmEnabled;
        return this;
    }

    public AMQPPublisherBuilder metricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
        return this;
    }

    @Override
    protected AMQPTransport buildTransport() {
        if (getSharedConnection() != null) {
            return new AMQPTransport(getSharedConnection());
        } else {
            return new AMQPTransport(ssl, host, port, metricsCollector);
        }
    }

    @Override
    protected AMQPConnectionProperties buildConnectionProperties() {
        return new AMQPConnectionProperties(username, password, virtualHost, connectionTimeout,
                heartbeatInterval, automaticRecoveryEnabled);
    }

    @Override
    protected AMQPPublishProperties buildPublishProperties() {
        return AMQPPublishProperties.builder()
                .exchange(exchange)
                .routingKey(routingKey)
                .timeout(publishTimeout)
                .confirmEnabled(confirmEnabled)
                .build();
    }

    @Override
    protected AMQPPublishContext buildPublishContext(AMQPTransport transport
                                                   , AMQPConnectionProperties connectionProperties
                                                   , AMQPPublishProperties publishProperties) {
        return new AMQPPublishContext(transport, connectionProperties, publishProperties);
    }

    @Override
    protected void validate() {
        assertNotNull(exchange, "exchange");
        assertNotNull(routingKey, "routingKey");
    }
}
