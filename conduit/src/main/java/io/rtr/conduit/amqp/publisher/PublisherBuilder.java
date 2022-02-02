package io.rtr.conduit.amqp.publisher;

import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportConnectionProperties;
import io.rtr.conduit.amqp.transport.TransportPublishContext;
import io.rtr.conduit.amqp.transport.TransportPublishProperties;
import io.rtr.conduit.amqp.validation.Validatable;

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
