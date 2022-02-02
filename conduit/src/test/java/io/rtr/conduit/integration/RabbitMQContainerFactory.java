package io.rtr.conduit.integration;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public final class RabbitMQContainerFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContainerFactory.class);

    private RabbitMQContainerFactory() {

    }

    public static RabbitMQContainer createBrokerWithSingleExchangeAndQueue() {
        return new RabbitMQContainer("rabbitmq:3-management")
            .withReuse(true)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withVhost("local")
            .withExchange(IntegrationTestHelpers.EXCHANGE, "direct")
            .withQueue(IntegrationTestHelpers.QUEUE)
            .withBinding(IntegrationTestHelpers.EXCHANGE, IntegrationTestHelpers.QUEUE)
            .withUser("guest", "guest");
    }
}
