package io.rtr.conduit.amqp.consumer;

import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportListenContext;

import java.io.IOException;

/**
 * The consumer operates in terms of a listen context; an encapsulation of a concrete transport and
 * its properties.
 */
public class Consumer implements AutoCloseable {
    private TransportListenContext transportContext;

    // ! Public interface.

    Consumer(final TransportListenContext transportContext) {
        this.transportContext = transportContext;
    }

    public void connect() throws IOException {
        this.getTransport().connect(transportContext.getConnectionProperties());
    }

    public boolean isConnected() {
        return this.getTransport().isConnected();
    }

    @Override
    public void close() throws IOException {
        this.getTransport().close();
    }

    public void listen() throws IOException {
        this.getTransport().listen(transportContext.getListenProperties());
    }

    public void stop() throws IOException {
        this.getTransport().stop();
    }

    public boolean isStopped(final int maxWaitMilliseconds) throws InterruptedException {
        return this.getTransport().isStopped(maxWaitMilliseconds);
    }

    private Transport getTransport() {
        return transportContext.getTransport();
    }
}
