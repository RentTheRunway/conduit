package io.rtr.conduit.amqp.publisher;

import io.rtr.conduit.amqp.transport.TransportMessageBundle;
import io.rtr.conduit.amqp.transport.TransportPublishContext;
import io.rtr.conduit.amqp.transport.TransportPublishProperties;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

/**
 * The publisher operates in terms of a publish context; an encapsulation of a concrete transport
 * and its properties. Example:
 *
 * <p>AMQPPublishContext context = new AMQPPublishContext( username, password, virtualHost,
 * exchange, routingKey, host, port );
 *
 * <p>Publisher publisher = new Publisher(context);
 */
public class Publisher implements AutoCloseable {
    private TransportPublishContext transportContext;

    // ! Public interface.
    Publisher(TransportPublishContext transportContext) {
        this.transportContext = transportContext;
    }

    // ! Connects to the context-specified host with context-specified credentials.
    public void connect() throws IOException {
        transportContext.getTransport().connect(transportContext.getConnectionProperties());
    }

    public boolean isConnected() {
        return transportContext.getTransport().isConnected();
    }

    @Override
    public void close() throws IOException {
        transportContext.getTransport().close();
    }

    /**
     * Publish the message using the publish properties defined in the transport context
     *
     * @param messageBundle Message to send
     */
    public boolean publish(TransportMessageBundle messageBundle)
            throws IOException, TimeoutException, InterruptedException {
        return publish(messageBundle, null);
    }

    /**
     * Publish the message with the publish properties passed as parameter, instead of the ones
     * defined in the transport context. Useful when those properties change from one message to
     * another (i.e. send messages using different routing keys)
     *
     * @param messageBundle Message to send
     * @param overridePublishProperties Publish properties to use when sending the message. If null,
     *     the ones defined in the transport context will be used.
     * @return
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public boolean publish(
            TransportMessageBundle messageBundle,
            TransportPublishProperties overridePublishProperties)
            throws IOException, TimeoutException, InterruptedException {
        if (overridePublishProperties == null) {
            overridePublishProperties = transportContext.getPublishProperties();
        }
        return transportContext.getTransport().publish(messageBundle, overridePublishProperties);
    }

    public <E> boolean transactionalPublish(Collection<E> messageBundles)
            throws IOException, TimeoutException, InterruptedException {
        return transportContext
                .getTransport()
                .transactionalPublish(messageBundles, transportContext.getPublishProperties());
    }
}
