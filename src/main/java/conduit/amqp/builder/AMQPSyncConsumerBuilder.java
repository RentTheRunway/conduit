package conduit.amqp.builder;

import conduit.amqp.AMQPCommonListenProperties;
import conduit.amqp.AMQPConnectionProperties;
import conduit.amqp.AMQPConsumerCallback;
import conduit.amqp.AMQPListenContext;
import conduit.amqp.AMQPListenProperties;
import conduit.amqp.AMQPTransport;

public class AMQPSyncConsumerBuilder extends AMQPConsumerBuilder<AMQPTransport
                                                               , AMQPListenProperties
                                                               , AMQPSyncConsumerBuilder> {
    private AMQPConsumerCallback callback;

    public static AMQPSyncConsumerBuilder builder() {
        return new AMQPSyncConsumerBuilder();
    }

    private AMQPSyncConsumerBuilder() {
    }

    public AMQPSyncConsumerBuilder callback(AMQPConsumerCallback callback) {
        this.callback = callback;
        return this;
    }

    @Override
    protected AMQPTransport buildTransport() {
        return new AMQPTransport(getHost(), getPort());
    }

    @Override
    protected AMQPListenProperties buildListenProperties() {
        AMQPCommonListenProperties commonListenProperties = new AMQPCommonListenProperties.AMQPCommonListenPropertiesBuilder()
                .setExchange(getExchange())
                .setQueue(getQueue())
                .setPrefetchCount(1)
                .setThreshold(getRetryThreshold())
                .setPoisonQueueEnabled(isPoisonQueueEnabled()).createAMQPCommonListenProperties();
        return new AMQPListenProperties(callback, commonListenProperties);
    }

    @Override
    protected AMQPListenContext buildListenContext(AMQPTransport transport
                                                 , AMQPConnectionProperties connectionProperties
                                                 , AMQPListenProperties listenProperties) {
        return new AMQPListenContext(transport, connectionProperties, listenProperties);
    }
}
