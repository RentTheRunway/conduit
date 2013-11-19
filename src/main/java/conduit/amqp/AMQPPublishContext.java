package conduit.amqp;


import conduit.transport.Transport;
import conduit.transport.TransportConnectionProperties;
import conduit.transport.TransportPublishContext;
import conduit.transport.TransportPublishProperties;

public class AMQPPublishContext implements TransportPublishContext {
    private AMQPTransport transport;
    private AMQPConnectionProperties connectionProperties;
    private AMQPPublishProperties publishProperties;

    public AMQPPublishContext(
            String username
          , String password
          , String virtualHost
          , String exchange
          , String routingKey
          , String host
          , int port
          , long timeout
    ) {
        connectionProperties = new AMQPConnectionProperties(username, password, virtualHost);
        publishProperties = new AMQPPublishProperties(exchange, routingKey, timeout);
        transport = new AMQPTransport(host, port);
    }

    public AMQPPublishContext(
            String username
          , String password
          , String exchange
          , String routingKey
          , String host
          , int port
    ) {
        this(username, password, "/", exchange, routingKey, host, port, 100);
    }

    public AMQPPublishContext(
            AMQPTransport transport
          , AMQPConnectionProperties connectionProperties
          , AMQPPublishProperties publishProperties
    ) {
        this.transport = transport;
        this.connectionProperties = connectionProperties;
        this.publishProperties = publishProperties;
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
    public TransportPublishProperties getPublishProperties() {
        return publishProperties;
    }
}
