package io.rtr.conduit.adapter;

public interface PublisherBuilder {

    public PublisherBuilder withProperties(java.util.Properties properties);

    public Publisher build();
}
