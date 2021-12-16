package io.rtr.conduit.adapter;

public interface MessageFuture<T> {
    T get();
}
