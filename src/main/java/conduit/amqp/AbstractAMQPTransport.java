package conduit.amqp;

import conduit.transport.Transport;
import conduit.transport.TransportListenProperties;

public abstract class AbstractAMQPTransport extends Transport{
    protected abstract AMQPQueueConsumer getConsumer(Object callback, AMQPCommonListenProperties commonListenProperties, String poisonPrefix);
    protected abstract AMQPCommonListenProperties getCommonListenProperties(TransportListenProperties properties);
    protected abstract Object getConsumerCallback(TransportListenProperties properties);
}
