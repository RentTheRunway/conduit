package conduit.transport;

/**
 * User: kmandrika
 * Date: 1/9/13
 */
public interface TransportListenContext {
    Transport getTransport();
    TransportConnectionProperties getConnectionProperties();
    TransportListenProperties getListenProperties();
}
