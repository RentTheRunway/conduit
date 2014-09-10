package io.rtr.conduit.amqp;

import io.rtr.conduit.amqp.impl.AMQPCommonListenProperties;
import io.rtr.conduit.amqp.impl.AMQPQueueConsumer;
import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

public abstract class AbstractAMQPTransport extends Transport{
    protected abstract AMQPQueueConsumer getConsumer(Object callback, AMQPCommonListenProperties commonListenProperties, String poisonPrefix);
    protected abstract AMQPCommonListenProperties getCommonListenProperties(TransportListenProperties properties);
    protected abstract Object getConsumerCallback(TransportListenProperties properties);
}
