package io.rtr.conduit.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public final class RabbitMQContainerFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContainerFactory.class);

    private RabbitMQContainerFactory() {}

    public static RabbitMQContainer createBrokerWithSingleExchangeAndQueue() {
        return createBrokerWithSingleExchangeAndQueue("rabbitmq:3-management");
    }

    /**
     * @param dockerImageName The name of the RabbitMQ Docker image; e.g., {@code rabbitmq:3-management}.
     */
    public static RabbitMQContainer createBrokerWithSingleExchangeAndQueue(final String dockerImageName) {
        return new RabbitMQContainer(dockerImageName)
                .withReuse(true)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withVhost("local")
                .withExchange(IntegrationTestHelpers.EXCHANGE, "direct")
                .withQueue(IntegrationTestHelpers.QUEUE)
                .withBinding(IntegrationTestHelpers.EXCHANGE, IntegrationTestHelpers.QUEUE)
                .withUser("guest", "guest");
    }
}
