package io.rtr.conduit.amqp.transport;

public interface TransportListenContext {
    Transport getTransport();

    TransportConnectionProperties getConnectionProperties();

    TransportListenProperties getListenProperties();
}
