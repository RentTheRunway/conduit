package io.rtr.conduit.integration;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.consumer.Consumer;
import io.rtr.conduit.amqp.impl.AMQPConnection;
import io.rtr.conduit.amqp.impl.AMQPConnectionProperties;
import io.rtr.conduit.amqp.impl.AMQPConsumerBuilder;
import io.rtr.conduit.amqp.impl.AMQPPublisherBuilder;
import io.rtr.conduit.amqp.publisher.Publisher;
import org.testcontainers.containers.RabbitMQContainer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public final class IntegrationTestHelpers {
    public static final String EXCHANGE = "foo";
    public static final String QUEUE = "queue";
    public static final String ROUTING_KEY = "bar";

    private IntegrationTestHelpers() {

    }

    public static void connectResources(Publisher publisher, Consumer consumer) {
        try {
            publisher.connect();
            consumer.connect();
            consumer.listen();

            assertTrue(publisher.isConnected());
            assertTrue(consumer.isConnected());
        } catch (IOException e) {
            throw new AssertionError("Publishing threw an IOException", e);
        }
    }

    public static void publishMessage(Publisher publisher, AMQPMessageBundle message) {
        try {
            publisher.publish(message);
        } catch (IOException | TimeoutException | InterruptedException e) {
            throw new AssertionError("Publishing threw an exception", e);
        }
    }

    public static AMQPConnection buildConnection(RabbitMQContainer rabbitMQContainer) {
        AMQPConnection conn = new AMQPConnection(false, rabbitMQContainer.getHost(), rabbitMQContainer.getAmqpPort(), null);
        AMQPConnectionProperties props = AMQPConnectionProperties.builder()
            .username(rabbitMQContainer.getAdminUsername())
            .password(rabbitMQContainer.getAdminPassword())
            .automaticRecoveryEnabled(true)
            .virtualHost("local")
            .build();
        try {
            conn.connect(props);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return conn;
    }

    public static Consumer buildConsumer(RabbitMQContainer rabbitMQContainer,
                                         AMQPConsumerCallback callback) {
        return buildConsumer(rabbitMQContainer.getHost(),
            rabbitMQContainer.getAmqpPort(),
            rabbitMQContainer.getAdminUsername(),
            rabbitMQContainer.getAdminPassword(),
            callback
        );
    }

    public static Consumer buildConsumer(String host,
                                         int port,
                                         String username,
                                         String password,
                                         AMQPConsumerCallback callback) {
        return AMQPConsumerBuilder.synchronous()
            .ssl(false)
            .host(host)
            .port(port)
            .virtualHost("local")
            .username(username)
            .password(password)
            .exchange(EXCHANGE)
            .autoCreateAndBind(EXCHANGE, AMQPConsumerBuilder.ExchangeType.DIRECT, QUEUE, ROUTING_KEY)
            .heartbeatInterval(1)
            .automaticRecoveryEnabled(true)
            .callback(callback)
            .build();
    }

    public static Consumer buildConsumerWithSharedConnection(AMQPConnection connection, AMQPConsumerCallback callback) {
        return AMQPConsumerBuilder.synchronous()
            .exchange(EXCHANGE)
            .autoCreateAndBind(EXCHANGE, AMQPConsumerBuilder.ExchangeType.DIRECT, QUEUE, ROUTING_KEY)
            .automaticRecoveryEnabled(true)
            .callback(callback)
            .sharedConnection(connection)
            .build();
    }

    public static Consumer buildConsumerWithAutoDeleteQueue(RabbitMQContainer rabbitMQContainer,
                                                            AMQPConsumerCallback callback) {
        return AMQPConsumerBuilder.synchronous()
            .ssl(false)
            .host(rabbitMQContainer.getHost())
            .port(rabbitMQContainer.getAmqpPort())
            .virtualHost("local")
            .username("guest")
            .password("guest")
            .autoCreateAndBind(EXCHANGE, AMQPConsumerBuilder.ExchangeType.DIRECT, "auto-delete-queue", true, ROUTING_KEY)
            .automaticRecoveryEnabled(true)
            .callback(callback)
            .build();
    }

    public static Publisher buildPublisher(RabbitMQContainer rabbitMQContainer) {
        return buildPublisher(rabbitMQContainer.getHost(),
            rabbitMQContainer.getAmqpPort(),
            rabbitMQContainer.getAdminUsername(),
            rabbitMQContainer.getAdminUsername()
        );
    }

    public static Publisher buildPublisher(String host, int port, String username, String password) {
        return AMQPPublisherBuilder.builder()
            .ssl(false)
            .host(host)
            .port(port)
            .virtualHost("local")
            .username(username)
            .password(password)
            .exchange(EXCHANGE)
            .routingKey(ROUTING_KEY)
            .heartbeatInterval(1)
            .automaticRecoveryEnabled(true)
            .build();
    }

    public static Publisher buildPublisherWithSharedConnection(AMQPConnection connection) {
        return AMQPPublisherBuilder.builder()
            .exchange(EXCHANGE)
            .routingKey(ROUTING_KEY)
            .automaticRecoveryEnabled(true)
            .sharedConnection(connection)
            .build();
    }
}
