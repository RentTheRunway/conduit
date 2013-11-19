package conduit.amqp.builder;

import conduit.amqp.AMQPAsyncConsumerCallback;
import conduit.amqp.AMQPAsyncListenProperties;
import conduit.amqp.AMQPAsyncTransport;
import conduit.amqp.AMQPConnectionProperties;
import conduit.amqp.AMQPListenContext;

public class AMQPAsyncConsumerBuilder extends AMQPConsumerBuilder<AMQPAsyncTransport
                                                                , AMQPAsyncListenProperties
                                                                , AMQPAsyncConsumerBuilder> {
    private AMQPAsyncConsumerCallback callback;

    public static AMQPAsyncConsumerBuilder builder() {
        return new AMQPAsyncConsumerBuilder();
    }

    private AMQPAsyncConsumerBuilder() {
    }

    public AMQPAsyncConsumerBuilder callback(AMQPAsyncConsumerCallback callback) {
        this.callback = callback;
        return this;
    }

    @Override
    protected AMQPAsyncTransport buildTransport() {
        return new AMQPAsyncTransport(getHost(), getPort());
    }

    @Override
    protected AMQPAsyncListenProperties buildListenProperties() {
        return new AMQPAsyncListenProperties(callback, getExchange(), getQueue());
    }

    @Override
    protected AMQPListenContext buildListenContext(AMQPAsyncTransport transport
                                                 , AMQPConnectionProperties connectionProperties
                                                 , AMQPAsyncListenProperties listenProperties) {
        return new AMQPListenContext(transport, connectionProperties, listenProperties);
    }
}
