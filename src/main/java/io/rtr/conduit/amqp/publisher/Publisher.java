package io.rtr.conduit.amqp.publisher;

import io.rtr.conduit.amqp.transport.TransportMessageBundle;
import io.rtr.conduit.amqp.transport.TransportPublishContext;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

/**
 * The io.rtr.conduit.amqp.publisher operates in terms of a publish context; an encapsulation of a
 * concrete transport and its properties.
 * Example:
 *
 * AMQPPublishContext context = new AMQPPublishContext(
 *         username, password, virtualHost, exchange, routingKey, host, port
 * );
 *
 * Publisher io.rtr.conduit.amqp.publisher = new Publisher(context);
 */
public class Publisher {
    private TransportPublishContext transportContext;

    //! Public interface.
    Publisher(TransportPublishContext transportContext) {
        this.transportContext = transportContext;
    }

    //! Connects to the context-specified host with context-specified credentials.
    public void connect() throws IOException {
        transportContext.getTransport().connect(transportContext.getConnectionProperties());
    }

    public void close() throws IOException {
        transportContext.getTransport().close();
    }

    public boolean publish(TransportMessageBundle messageBundle)
            throws IOException, TimeoutException, InterruptedException {
        return transportContext.getTransport().publish(messageBundle, transportContext.getPublishProperties());
    }

    public <E> boolean transactionalPublish(Collection<E> messageBundles)
            throws IOException, TimeoutException, InterruptedException {
        return transportContext.getTransport().transactionalPublish(messageBundles, transportContext.getPublishProperties());
    }
}
