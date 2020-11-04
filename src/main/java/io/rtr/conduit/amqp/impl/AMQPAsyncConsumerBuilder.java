package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.MetricsCollector;
import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;

public class AMQPAsyncConsumerBuilder extends AMQPConsumerBuilder<AMQPAsyncTransport
        , AMQPAsyncListenProperties
        , AMQPAsyncConsumerBuilder> {
    private AMQPAsyncConsumerCallback callback;
    private MetricsCollector metricsCollector;

    public static AMQPAsyncConsumerBuilder builder() {
        return new AMQPAsyncConsumerBuilder();
    }

    private AMQPAsyncConsumerBuilder() {
        super.prefetchCount(100);
    }

    public AMQPAsyncConsumerBuilder callback(AMQPAsyncConsumerCallback callback) {
        this.callback = callback;
        return this;
    }

    public AMQPAsyncConsumerBuilder metricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
        return this;
    }

    @Override
    protected AMQPAsyncTransport buildTransport() {
        if (getSharedConnection() != null) {
            return new AMQPAsyncTransport(getSharedConnection());
        } else {
            return new AMQPAsyncTransport(isSsl(), getHost(), getPort(), metricsCollector);
        }
    }

    @Override
    protected AMQPAsyncListenProperties buildListenProperties() {
        return new AMQPAsyncListenProperties(callback,
                getExchange(),
                getQueue(),
                getRetryThreshold(),
                getPrefetchCount(),
                isPoisonQueueEnabled(),
                shouldPurgeOnConnect(),
                isDynamicQueueCreation(),
                getPoisonPrefix(),
                getDynamicQueueRoutingKey(),
                isAutoCreateAndBind(),
                getExchangeType(),
                getRoutingKey());
    }

    @Override
    protected AMQPListenContext buildListenContext(AMQPAsyncTransport transport
            , AMQPConnectionProperties connectionProperties
            , AMQPAsyncListenProperties amqpAsyncListenProperties) {
        return new AMQPListenContext(transport, connectionProperties, amqpAsyncListenProperties);
    }
}
