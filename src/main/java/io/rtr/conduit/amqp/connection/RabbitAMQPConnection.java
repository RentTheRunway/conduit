package io.rtr.conduit.amqp.connection;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.rtr.conduit.amqp.impl.AMQPPublisherBuilder;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitAMQPConnection extends AMQPConnection {

    private Connection connection;
    private final ConnectionFactory connectionFactory;

    RabbitAMQPConnection(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void connect() throws IOException, TimeoutException {
        this.connection = this.connectionFactory.newConnection();
    }

    public Connection getConnection(){
        return this.connection;
    }

    // for backwards compatibility for testing
    public void setConnection(Connection connection){
        this.connection = connection;
    }
}

