package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.MetricsCollector;
import io.rtr.conduit.amqp.AMQPConsumerCallback;

public class AMQPSyncConsumerBuilder extends AMQPConsumerBuilder<AMQPTransport
        , AMQPListenProperties
        , AMQPSyncConsumerBuilder> {
    private AMQPConsumerCallback callback;
    private MetricsCollector metricsCollector;

    public static AMQPSyncConsumerBuilder builder() {
        return new AMQPSyncConsumerBuilder();
    }

    private AMQPSyncConsumerBuilder() {
        super.prefetchCount(1);
    }

    public AMQPSyncConsumerBuilder callback(AMQPConsumerCallback callback) {
        this.callback = callback;
        return this;
    }

    public AMQPSyncConsumerBuilder metricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
        return this;
    }

    @Override
    protected AMQPTransport buildTransport() {
        if (getSharedConnection() != null) {
            return new AMQPTransport(getSharedConnection());
        } else {
            return new AMQPTransport(isSsl(), getHost(), getPort(), metricsCollector);
        }
    }

    @Override
    protected AMQPListenProperties buildListenProperties() {
        return new AMQPListenProperties(callback,
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
    protected AMQPListenContext buildListenContext(AMQPTransport transport
            , AMQPConnectionProperties connectionProperties
            , AMQPListenProperties listenProperties) {
        return new AMQPListenContext(transport, connectionProperties, listenProperties);
    }
}
