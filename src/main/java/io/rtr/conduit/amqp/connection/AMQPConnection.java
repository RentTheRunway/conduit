package io.rtr.conduit.amqp.connection;

import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public abstract class AMQPConnection {
    public abstract void connect() throws IOException, TimeoutException;
}
