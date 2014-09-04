package conduit.amqp;


import conduit.transport.TransportListenProperties;

public class AMQPAsyncListenProperties implements TransportListenProperties {
    private AMQPAsyncConsumerCallback callback;
    private AMQPCommonListenProperties commonListenProperties;

    public AMQPAsyncListenProperties(AMQPAsyncConsumerCallback callback, AMQPCommonListenProperties commonListenProperties) {
        this.callback = callback;
        this.commonListenProperties = commonListenProperties;
    }

    public AMQPAsyncConsumerCallback getCallback() {
        return callback;
    }

    public AMQPCommonListenProperties getCommonListenProperties() {
        return commonListenProperties;
    }
}
