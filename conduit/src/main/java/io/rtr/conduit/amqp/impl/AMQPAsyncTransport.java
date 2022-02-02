package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.MetricsCollector;
import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

public class AMQPAsyncTransport extends AMQPTransport {
    public AMQPAsyncTransport(boolean ssl, String host, int port, MetricsCollector metricsCollector) {
        super(ssl, host, port, metricsCollector);
    }

    public AMQPAsyncTransport(AMQPConnection sharedConnection) {
        super(sharedConnection);
    }

    @Override
    protected AMQPQueueConsumer getConsumer(Object callback, AMQPCommonListenProperties commonListenProperties, String poisonPrefix){
        return new AMQPAsyncQueueConsumer(
                getChannel(),
                (AMQPAsyncConsumerCallback) callback,
                commonListenProperties.getThreshold(),
                poisonPrefix,
                commonListenProperties.isPoisonQueueEnabled()
        );
    }

    @Override
    protected AMQPCommonListenProperties getCommonListenProperties(TransportListenProperties properties) {
        return (AMQPCommonListenProperties) properties;
    }

    @Override
    protected Object getConsumerCallback(TransportListenProperties properties) {
        return ((AMQPAsyncListenProperties)properties).getCallback();
    }
}
