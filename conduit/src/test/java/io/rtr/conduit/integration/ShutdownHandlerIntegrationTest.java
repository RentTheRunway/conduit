package io.rtr.conduit.integration;

import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.consumer.Consumer;
import io.rtr.conduit.amqp.publisher.Publisher;
import io.rtr.conduit.util.RecordingAmqpCallbackHandler;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
public class ShutdownHandlerIntegrationTest {

    private static final Network COMMON_NETWORK = Network.newNetwork();
    public static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    @Container
    private static final RabbitMQContainer RABBIT_MQ_CONTAINER = RabbitMQContainerFactory.createBrokerWithSingleExchangeAndQueue()
        .withExposedPorts(5672)
        .withNetwork(COMMON_NETWORK);
    @Container
    private static final ToxiproxyContainer TOXI_PROXY = new ToxiproxyContainer("shopify/toxiproxy:2.1.4")
        .withNetwork(COMMON_NETWORK)
        .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

    @Test
    public void testReconnectAfterBrokerShutdown() throws IOException {
        ToxiproxyContainer.ContainerProxy proxyInterface = TOXI_PROXY.getProxy(RABBIT_MQ_CONTAINER, 5672);

        RecordingAmqpCallbackHandler callbackHandler = new RecordingAmqpCallbackHandler();
        Publisher publisher;
        Consumer consumer;

        // Simulating a fresh existing container ready for us to interact with
        publisher = IntegrationTestHelpers.buildPublisher(
            proxyInterface.getContainerIpAddress(),
            proxyInterface.getProxyPort(),
            RABBIT_MQ_CONTAINER.getAdminUsername(),
            RABBIT_MQ_CONTAINER.getAdminPassword()
        );
        consumer = IntegrationTestHelpers.buildConsumer(
            proxyInterface.getContainerIpAddress(),
            proxyInterface.getProxyPort(),
            RABBIT_MQ_CONTAINER.getAdminUsername(),
            RABBIT_MQ_CONTAINER.getAdminPassword(),
            callbackHandler
        );
        IntegrationTestHelpers.connectResources(publisher, consumer);

        // Simulating the RabbitMQ broker disappearing (i.e. a crash or network issue)
        // We should react here and notify status of the connection to the broker, have auto-connection try
        proxyInterface.setConnectionCut(true);
        awaitDisconnection(publisher, consumer);

        // Simulating RabbitMQ restarting and coming back online, we should auto-reconnect
        proxyInterface.setConnectionCut(false);
        awaitReconnection(publisher, consumer);
        IntegrationTestHelpers.publishMessage(publisher, new AMQPMessageBundle("Hello World!"));

        // Message publication should still work after reconnecting
        await()
            .timeout(1, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() ->  callbackHandler.getCapturedMessages().size(), is(equalTo(1)));
        assertEquals("Hello World!", callbackHandler.getCapturedMessages().stream().findAny()
            .map(AMQPMessageBundle::getBody)
            .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
            .orElseThrow(() -> new AssertionFailedError("Expected to have received a message 'Hello World!'"))
        );

        // Cleanup, close any spurious connections
        consumer.close();
        publisher.close();
    }

    private void awaitDisconnection(Publisher publisher, Consumer consumer) {
        await()
            .alias("Checking for publisher disconnection")
            .timeout(10, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .failFast(() -> publisher == null)
            .until(publisher::isConnected, value -> !value);
        await()
            .alias("Checking for consumer disconnection")
            .timeout(10, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .failFast(() -> consumer == null)
            .until(consumer::isConnected, value -> !value);
    }

    private void awaitReconnection(Publisher publisher, Consumer consumer) {
        await()
            .alias("Checking for publisher re-connection")
            .timeout(10, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .failFast(() -> publisher == null)
            .until(publisher::isConnected);
        await()
            .alias("Checking for consumer re-connection")
            .timeout(10, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .failFast(() -> consumer == null)
            .until(consumer::isConnected);
    }
}
