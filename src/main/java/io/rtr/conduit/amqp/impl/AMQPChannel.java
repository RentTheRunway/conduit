package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.connection.AMQPConnection;

public class AMQPChannel {
    private final AMQPConnection connection;

    public AMQPChannel(AMQPConnection connection){
        this.connection = connection;
    }


}
