package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.SocketUtil;
import io.rtr.conduit.amqp.publisher.Publisher;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static io.rtr.conduit.amqp.SystemPropertiesUtil.withSystemProperty;

public class AMQPIntegrationIT {
    private static final String TRUST_STORE = Paths.get(
        System.getProperty("user.dir"),
        "target",
        "test-classes",
        "keystore.jks"
    ).toString();

    private static int PORT;
    private static final Broker BROKER;

    static {
        BROKER = new Broker();
    }

    @BeforeClass
    public static void fixtureSetUp() throws Exception {
        PORT = SocketUtil.getAvailablePort();
        final BrokerOptions options = new BrokerOptions();
        options.setConfigProperty("qpid.amqp_port", String.valueOf(PORT));
        options.setConfigProperty("qpid.broker.defaultPreferenceStoreAttributes", "{\"type\": \"Noop\"}");
        options.setConfigProperty("qpid.vhost", "local");
        options.setConfigProperty("qpid.keystore", TRUST_STORE);
        options.setConfigurationStoreType("Memory");
        options.setStartupLoggedToSystemOut(false);
        BROKER.startup(options);
    }

    @AfterClass
    public static void fixtureTearDown() {
        BROKER.shutdown();
    }

    private static void testPublisher(Consumer<AMQPPublisherBuilder> configuration) {
        final AMQPPublisherBuilder builder = AMQPPublisherBuilder.builder();
        configuration.accept(builder);
        final Publisher publisher = builder.build();
        try {
            publisher.connect();
            publisher.publish(new AMQPMessageBundle("a message"));
        } catch (IOException | TimeoutException | InterruptedException e) {
            // gotta catch 'em all!
            throw new AssertionError("Publishing threw an exception", e);
        }
    }

    @Test
    public void testSslAmqpTransport() {
        withSystemProperty("javax.net.ssl.trustStore", TRUST_STORE, () ->
            testPublisher(
                builder -> builder
                    .ssl(true)
                    .host("localhost")
                    .port(PORT)
                    .virtualHost("local")
                    .username("guest")
                    .password("guest")
                    .exchange("foo")
                    .routingKey("bar")
            )
        );
    }
}
