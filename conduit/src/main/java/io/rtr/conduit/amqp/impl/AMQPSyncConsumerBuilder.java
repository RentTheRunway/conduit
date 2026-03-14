package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.MetricsCollector;

import io.rtr.conduit.amqp.AMQPConsumerCallback;

public class AMQPSyncConsumerBuilder
        extends AMQPConsumerBuilder<AMQPTransport, AMQPListenProperties, AMQPSyncConsumerBuilder> {
    private AMQPConsumerCallback callback;
    private MetricsCollector metricsCollector;

    public static AMQPSyncConsumerBuilder builder() {
        return new AMQPSyncConsumerBuilder();
    }

    protected AMQPSyncConsumerBuilder() {
        super.prefetchCount(1);
    }

    public AMQPSyncConsumerBuilder callback(final AMQPConsumerCallback callback) {
        this.callback = callback;
        return this;
    }

    public AMQPSyncConsumerBuilder metricsCollector(final MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
        return this;
    }

    @Override
    protected AMQPTransport buildTransport() {
        if (this.getSharedConnection() != null) {
            return new AMQPTransport(this.getSharedConnection());
        } else {
            return new AMQPTransport(
                    this.isSsl(), this.getHost(), this.getPort(), metricsCollector);
        }
    }

    @Override
    protected AMQPListenProperties buildListenProperties() {
        return new AMQPListenProperties(
                callback,
                this.getExchange(),
                this.getQueue(),
                this.isAutoDeleteQueue(),
                this.getRetryThreshold(),
                this.isPoisonQueueEnabled(),
                this.shouldPurgeOnConnect(),
                this.getPrefetchCount(),
                this.getPoisonPrefix(),
                this.getDynamicQueueRoutingKey(),
                this.isDynamicQueueCreation(),
                this.getExchangeType(),
                this.getRoutingKey(),
                this.isAutoCreateAndBind(),
                this.getExclusive());
    }

    @Override
    protected AMQPListenContext buildListenContext(
            final AMQPTransport transport,
            final AMQPConnectionProperties connectionProperties,
            final AMQPListenProperties listenProperties) {
        return new AMQPListenContext(transport, connectionProperties, listenProperties);
    }
}
