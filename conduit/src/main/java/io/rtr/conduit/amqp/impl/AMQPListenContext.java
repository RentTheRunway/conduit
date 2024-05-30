package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportConnectionProperties;
import io.rtr.conduit.amqp.transport.TransportListenContext;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

public class AMQPListenContext implements TransportListenContext {
    private AMQPTransport transport;
    private AMQPConnectionProperties connectionProperties;
    private TransportListenProperties listenProperties;

    // ! Consume context.
    AMQPListenContext(
            AMQPTransport amqpTransport,
            AMQPConnectionProperties amqpConnectionProperties,
            AMQPCommonListenProperties amqpListenProperties) {
        connectionProperties = amqpConnectionProperties;
        listenProperties = amqpListenProperties;
        transport = amqpTransport;
    }

    @Override
    public Transport getTransport() {
        return transport;
    }

    @Override
    public TransportConnectionProperties getConnectionProperties() {
        return connectionProperties;
    }

    @Override
    public TransportListenProperties getListenProperties() {
        return listenProperties;
    }
}
