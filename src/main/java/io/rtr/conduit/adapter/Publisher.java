package io.rtr.conduit.adapter;

public interface Publisher {

    public MessageFuture publish(Object mesg);

    public void close();

    public Object getPublisherImpl();
}
