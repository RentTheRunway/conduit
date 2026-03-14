package io.rtr.conduit.amqp.impl;

public class ConduitConnectionStateException extends IllegalStateException {
    public ConduitConnectionStateException(final String s) {
        super(s);
    }
}
