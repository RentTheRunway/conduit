package conduit.consumer;

import conduit.transport.TransportListenContext;

import java.io.IOException;

/**
 * The consumer operates in terms of a listen context; an encapsulation of a
 * concrete transport and its properties.
 *
 * User: kmandrika
 * Date: 1/8/13
 */
public class Consumer {
    private TransportListenContext transportContext;

    //! Public interface.

    public Consumer(TransportListenContext transportContext) {
        this.transportContext = transportContext;
    }

    public void connect() throws IOException {
        transportContext.getTransport().connect(transportContext.getConnectionProperties());
    }

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
