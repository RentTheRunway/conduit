package conduit.transport;

/**
 * User: kmandrika
 * Date: 1/9/13
 */
public interface TransportPublishContext {
    Transport getTransport();
    TransportConnectionProperties getConnectionProperties();
    TransportPublishProperties getPublishProperties();
}
