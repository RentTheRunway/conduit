package io.rtr.conduit.amqp.consumer;

import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportConnectionProperties;
import io.rtr.conduit.amqp.transport.TransportListenContext;
import io.rtr.conduit.amqp.transport.TransportListenProperties;
import io.rtr.conduit.amqp.validation.Validatable;

public abstract class ConsumerBuilder<
                T extends Transport,
                C extends TransportConnectionProperties,
                L extends TransportListenProperties,
                LC extends TransportListenContext>
        extends Validatable {
    protected abstract T buildTransport();

    protected abstract C buildConnectionProperties();

    protected abstract L buildListenProperties();

    protected abstract LC buildListenContext(
            T transport, C connectionProperties, L listenProperties);

    public final Consumer build() {
        validate();
        LC context =
                buildListenContext(
                        buildTransport(), buildConnectionProperties(), buildListenProperties());
        return new Consumer(context);
    }
}
