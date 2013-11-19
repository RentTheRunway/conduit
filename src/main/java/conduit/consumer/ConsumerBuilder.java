package conduit.consumer;

import conduit.transport.Transport;
import conduit.transport.TransportConnectionProperties;
import conduit.transport.TransportListenContext;
import conduit.transport.TransportListenProperties;
import conduit.validation.Validatable;

public abstract class ConsumerBuilder<T extends Transport
                                    , C extends TransportConnectionProperties
                                    , L extends TransportListenProperties
                                    , LC extends TransportListenContext>
                extends Validatable {
    protected abstract T buildTransport();
    protected abstract C buildConnectionProperties();
    protected abstract L buildListenProperties();
    protected abstract LC buildListenContext(T transport, C connectionProperties, L listenProperties);

    public final Consumer build() {
        validate();
        LC context = buildListenContext(buildTransport(), buildConnectionProperties(), buildListenProperties());
        return new Consumer(context);
    }
}
