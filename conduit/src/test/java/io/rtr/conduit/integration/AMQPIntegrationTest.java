package io.rtr.conduit.integration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.consumer.Consumer;
import io.rtr.conduit.amqp.impl.AMQPConnection;
import io.rtr.conduit.amqp.publisher.Publisher;
import io.rtr.conduit.util.LoggingAmqpCallbackHandler;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
class AMQPIntegrationTest {
    @Container
    private static final RabbitMQContainer RABBITMQ_CONTAINER =
            RabbitMQContainerFactory.createBrokerWithSingleExchangeAndQueue();

    @Test
    void testSslAmqpTransport() {
        final AMQPMessageBundle message = new AMQPMessageBundle("a message");
        final Publisher publisher = IntegrationTestHelpers.buildPublisher(RABBITMQ_CONTAINER);
        final Consumer consumer =
                IntegrationTestHelpers.buildConsumer(
                        RABBITMQ_CONTAINER, new LoggingAmqpCallbackHandler());
        IntegrationTestHelpers.connectResources(publisher, consumer);
        IntegrationTestHelpers.publishMessage(publisher, message);

        try {
            consumer.close();
        } catch (final IOException e) {
            fail("Should not throw error when closing consumer", e);
        }
    }

    @Test
    void testAmqpTransportWithSharedConnection() throws IOException {
        final AMQPMessageBundle message = new AMQPMessageBundle("a message");
        final AMQPConnection connection =
                IntegrationTestHelpers.buildConnection(RABBITMQ_CONTAINER);

        try (final Consumer consumer =
                IntegrationTestHelpers.buildConsumerWithSharedConnection(
                        connection, new LoggingAmqpCallbackHandler())) {
            final Publisher publisher =
                    IntegrationTestHelpers.buildPublisherWithSharedConnection(connection);
            IntegrationTestHelpers.connectResources(publisher, consumer);
            IntegrationTestHelpers.publishMessage(publisher, message);
        } catch (final IOException e) {
            fail("Could not connect consumer to RabbitMQ broker", e);
        } finally {
            connection.disconnect();
        }
    }

    @Test
    void testManualReconnectAfterManualClose() {
        final AMQPConsumerCallback callback = mock(AMQPConsumerCallback.class);
        final AMQPMessageBundle message = new AMQPMessageBundle("a message");

        final Publisher publisher = IntegrationTestHelpers.buildPublisher(RABBITMQ_CONTAINER);
        final Consumer consumer =
                IntegrationTestHelpers.buildConsumer(RABBITMQ_CONTAINER, callback);
        IntegrationTestHelpers.connectResources(publisher, consumer);

        try {
            publisher.close();
            consumer.close();
        } catch (final IOException e) {
            fail("Error disconnecting publisher or consumer", e);
        }

        assertFalse(publisher.isConnected());
        assertFalse(consumer.isConnected());

        IntegrationTestHelpers.connectResources(publisher, consumer);

        IntegrationTestHelpers.publishMessage(publisher, message);
        Mockito.verify(callback, timeout(2000).times(1)).handle(any());

        try {
            consumer.close();
            publisher.close();
        } catch (final IOException e) {
            fail("Should not fail to close consumer/publisher at end of test", e);
        }
    }

    @Test
    void testAmqpTransportWithAutoDeleteQueue() throws IOException {
        final AMQPConsumerCallback callback = mock(AMQPConsumerCallback.class);
        final AMQPMessageBundle message = new AMQPMessageBundle("a message");
        final AMQPConnection connection =
                IntegrationTestHelpers.buildConnection(RABBITMQ_CONTAINER);

        try (final Consumer consumer =
                IntegrationTestHelpers.buildConsumerWithAutoDeleteQueue(
                        RABBITMQ_CONTAINER, callback)) {
            final Publisher publisher = IntegrationTestHelpers.buildPublisher(RABBITMQ_CONTAINER);
            IntegrationTestHelpers.connectResources(publisher, consumer);
            IntegrationTestHelpers.publishMessage(publisher, message);

            Mockito.verify(callback, timeout(500).times(1)).handle(any());
        } catch (final IOException e) {
            fail("Could not connect consumer to RabbitMQ broker", e);
        } finally {
            connection.disconnect();
        }
    }
}
