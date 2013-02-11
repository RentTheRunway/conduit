package conduit.transport;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

/**
 * User: kmandrika
 * Date: 1/8/13
 */
public abstract class Transport {
    //! Public interface.

    //! Establishes a connection to either an intermediary or the other
    //  end point. In the case of AMQP, this method is used to connect
    //  to the broker.
    public final void connect(TransportConnectionProperties properties) throws IOException {
        connectImpl(properties);
    }

    //! Closes the connection.
    public final void close() throws IOException {
        closeImpl();
    }

    //! Starts the asynchronous delivery mechanism.
    public final void listen(TransportListenProperties properties) throws IOException {
        listenImpl(properties);
    }

    //! Stops listening for incoming messages.
    public final void stop() throws IOException {
        stopImpl();
    }

    //! Publish a message to the other endpoint.
    public final boolean publish(TransportMessageBundle messageBundle, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        return publishImpl(messageBundle, properties);
    }

    public final <E> boolean transactionalPublish(Collection<E> messageBundles, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        return transactionalPublishImpl(messageBundles, properties);
    }

    //! Implementation

    protected abstract void connectImpl(TransportConnectionProperties properties) throws IOException;
    protected abstract void closeImpl() throws IOException;

    protected void listenImpl(TransportListenProperties properties) throws IOException {}
    protected void stopImpl() throws IOException {}
    protected boolean publishImpl(TransportMessageBundle messageBundle, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException { return false; }
    protected <E> boolean transactionalPublishImpl(Collection<E> messageBundles, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException { return false; }
}
