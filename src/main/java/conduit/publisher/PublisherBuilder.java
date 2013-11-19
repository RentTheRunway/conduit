package conduit.publisher;

import conduit.transport.Transport;
import conduit.transport.TransportConnectionProperties;
import conduit.transport.TransportPublishContext;
import conduit.transport.TransportPublishProperties;
import conduit.validation.Validatable;

public abstract class PublisherBuilder<T extends Transport, C extends TransportConnectionProperties, P extends TransportPublishProperties, PC extends TransportPublishContext>
                extends Validatable {

    protected abstract T buildTransport();
    protected abstract C buildConnectionProperties();
    protected abstract P buildPublishProperties();
    protected abstract PC buildPublishContext(T transport, C connectionProperties, P publishProperties);

    public final Publisher build() {
        validate();
        PC context = buildPublishContext(buildTransport(), buildConnectionProperties(), buildPublishProperties());
        return new Publisher(context);
    }
}
