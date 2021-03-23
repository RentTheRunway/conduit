package io.rtr.conduit.amqp.impl;

public class ConduitConnectionStateException extends IllegalStateException {
    public ConduitConnectionStateException(String s) {
        super(s);
    }
}
