package conduit.transport;

public interface TransportListenContext {
    Transport getTransport();
    TransportConnectionProperties getConnectionProperties();
    TransportListenProperties getListenProperties();
}
