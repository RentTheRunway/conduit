package conduit.amqp;

import conduit.transport.Transport;
import conduit.transport.TransportConnectionProperties;
import conduit.transport.TransportListenContext;
import conduit.transport.TransportListenProperties;

/**
 * User: kmandrika
 * Date: 1/9/13
 */
public class AMQPListenContext implements TransportListenContext {
    private AMQPTransport transport;
    private AMQPConnectionProperties connectionProperties;
    private AMQPListenProperties listenProperties;

    //! Consume context.
    public AMQPListenContext(
            String username
          , String password
          , String virtualHost
          , String exchange
          , String queue
          , String host
          , int port
          , AMQPConsumerCallback callback
    ) {
        connectionProperties = new AMQPConnectionProperties(username, password, virtualHost);
        listenProperties = new AMQPListenProperties(callback, exchange, queue);
        transport = new AMQPTransport(host, port);
    }

    public AMQPListenContext(
            String username
          , String password
          , String exchange
          , String queue
          , String host
          , int port
          , AMQPConsumerCallback callback
    ) {
        this(username, password, "/", exchange, queue, host, port, callback);
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
