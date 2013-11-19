package conduit.transport;

public interface TransportPublishContext {
    Transport getTransport();
    TransportConnectionProperties getConnectionProperties();
    TransportPublishProperties getPublishProperties();
}
