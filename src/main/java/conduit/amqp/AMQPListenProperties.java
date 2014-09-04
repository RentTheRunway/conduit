package conduit.amqp;

import conduit.transport.TransportListenProperties;

/**
 * There is a single restriction on the exchange/queue architecture; the consumer
 * queue must have a respective poison counterpart. Given a queue "Q", there must
 * exist a queue "Q.poison".
 */
public class AMQPListenProperties implements TransportListenProperties {
    private AMQPConsumerCallback callback;
    private AMQPCommonListenProperties commonListenProperties;

    public AMQPListenProperties(AMQPConsumerCallback callback, AMQPCommonListenProperties commonListenProperties) {
        this.callback = callback;
        this.commonListenProperties = commonListenProperties;
    }

    public AMQPConsumerCallback getCallback() {
        return callback;
    }

    public AMQPCommonListenProperties getCommonListenProperties() {
        return commonListenProperties;
    }
}
