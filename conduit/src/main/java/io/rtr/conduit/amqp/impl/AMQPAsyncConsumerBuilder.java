package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.MetricsCollector;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;

public class AMQPAsyncConsumerBuilder
        extends AMQPConsumerBuilder<
                AMQPAsyncTransport, AMQPAsyncListenProperties, AMQPAsyncConsumerBuilder> {
    private AMQPAsyncConsumerCallback callback;
    private MetricsCollector metricsCollector;

    public static AMQPAsyncConsumerBuilder builder() {
        return new AMQPAsyncConsumerBuilder();
    }

    protected AMQPAsyncConsumerBuilder() {
        super.prefetchCount(100);
    }

    public AMQPAsyncConsumerBuilder callback(final AMQPAsyncConsumerCallback callback) {
        this.callback = callback;
        return this;
    }

    public AMQPAsyncConsumerBuilder metricsCollector(final MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
        return this;
    }

    @Override
    protected AMQPAsyncTransport buildTransport() {
        if (this.getSharedConnection() != null) {
            return new AMQPAsyncTransport(this.getSharedConnection());
        } else {
            return new AMQPAsyncTransport(
                    this.isSsl(), this.getHost(), this.getPort(), metricsCollector);
        }
    }

    @Override
    protected AMQPAsyncListenProperties buildListenProperties() {
        return new AMQPAsyncListenProperties(
                callback,
                this.getExchange(),
                this.getQueue(),
                this.isAutoDeleteQueue(),
                this.getRetryThreshold(),
                this.getPrefetchCount(),
                this.isPoisonQueueEnabled(),
                this.shouldPurgeOnConnect(),
                this.isDynamicQueueCreation(),
                this.getPoisonPrefix(),
                this.getDynamicQueueRoutingKey(),
                this.isAutoCreateAndBind(),
                this.getExchangeType(),
                this.getRoutingKey(),
                this.getExclusive());
    }

    @Override
    protected AMQPListenContext buildListenContext(
            final AMQPAsyncTransport transport,
            final AMQPConnectionProperties connectionProperties,
            final AMQPAsyncListenProperties amqpAsyncListenProperties) {
        return new AMQPListenContext(transport, connectionProperties, amqpAsyncListenProperties);
    }
}
