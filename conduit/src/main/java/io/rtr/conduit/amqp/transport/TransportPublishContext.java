package io.rtr.conduit.amqp.transport;

public interface TransportPublishContext {
    Transport getTransport();

    TransportConnectionProperties getConnectionProperties();

    TransportPublishProperties getPublishProperties();
}
