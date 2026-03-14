package io.rtr.conduit.amqp.transport;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

public abstract class Transport {
    // ! Public interface.

    // ! Establishes a connection to either an intermediary or the other
    //  end point. In the case of AMQP, this method is used to connect
    //  to the broker.
    public final void connect(final TransportConnectionProperties properties) throws IOException {
        this.connectImpl(properties);
    }

    public boolean isConnected() {
        return this.isConnectedImpl();
    }

    // ! Closes the connection.
    public final void close() throws IOException {
        this.closeImpl();
    }

    // ! Starts the asynchronous delivery mechanism.
    public final void listen(final TransportListenProperties properties) throws IOException {
        this.listenImpl(properties);
    }

    // ! Stops listening for incoming messages.
    public final void stop() throws IOException {
        this.stopImpl();
    }

    // Is the listener thread pool still doing work? (stop is not synchronous)
    public final boolean isStopped(final int maxWaitMilliseconds) throws InterruptedException {
        return this.isStoppedImpl(maxWaitMilliseconds);
    }

    // ! Publish a message to the other endpoint.
    public final boolean publish(
            final TransportMessageBundle messageBundle, final TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        return this.publishImpl(messageBundle, properties);
    }

    public final <E> boolean transactionalPublish(
            final Collection<E> messageBundles, final TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        return this.transactionalPublishImpl(messageBundles, properties);
    }

    // ! Implementation

    protected abstract boolean isConnectedImpl();

    protected abstract void connectImpl(TransportConnectionProperties properties)
            throws IOException;

    protected abstract void closeImpl() throws IOException;

    protected void listenImpl(final TransportListenProperties properties) throws IOException {}

    protected void stopImpl() throws IOException {}

    protected abstract boolean isStoppedImpl(int waitMillSeconds) throws InterruptedException;

    protected boolean publishImpl(
            final TransportMessageBundle messageBundle, final TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        return false;
    }

    protected <E> boolean transactionalPublishImpl(
            final Collection<E> messageBundles, final TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        return false;
    }
}
