package conduit.amqp;

import conduit.transport.TransportListenProperties;

public class AMQPAsyncTransport extends AMQPTransport {
    public AMQPAsyncTransport(String host, int port) {
        super(host, port);
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
        AMQPAsyncListenProperties listenProperties = (AMQPAsyncListenProperties)properties;
        return listenProperties.getCommonListenProperties();
    }

    @Override
    protected Object getConsumerCallback(TransportListenProperties properties) {
        return ((AMQPAsyncListenProperties)properties).getCallback();
    }
}
