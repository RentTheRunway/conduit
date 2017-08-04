package io.rtr.conduit.amqp.consumer;

import io.rtr.conduit.amqp.transport.TransportListenContext;

import java.io.IOException;

/**
 * The consumer operates in terms of a listen context; an encapsulation of a
 * concrete transport and its properties.
 */
public class Consumer implements AutoCloseable {
    private TransportListenContext transportContext;

    //! Public interface.

    Consumer(TransportListenContext transportContext) {
        this.transportContext = transportContext;
    }

    public void connect() throws IOException {
        transportContext.getTransport().connect(transportContext.getConnectionProperties());
    }

    @Override
    public void close() throws IOException {
        transportContext.getTransport().close();
    }

    public void listen() throws IOException {
        transportContext.getTransport().listen(transportContext.getListenProperties());
    }

    public void stop() throws IOException {
        transportContext.getTransport().stop();
    }
}
