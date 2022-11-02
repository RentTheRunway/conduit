package io.rtr.conduit.amqp.consumer;

import com.rabbitmq.client.Channel;
import io.rtr.conduit.amqp.transport.Transport;
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
        getTransport().connect(transportContext.getConnectionProperties());
    }

    public boolean isConnected() {
        return getTransport().isConnected();
    }

    @Override
    public void close() throws IOException {
        getTransport().close();
    }

    public void listen() throws IOException {
        getTransport().listen(transportContext.getListenProperties());
    }

    public void stop() throws IOException {
        getTransport().stop();
    }

    public boolean isStopped(int maxWaitMilliseconds) throws InterruptedException {
        return getTransport().isStopped(maxWaitMilliseconds);
    }

    private Transport getTransport() {
        return transportContext.getTransport();
    }

    public Channel getChannel() { return getTransport().getChannel(); }
}
