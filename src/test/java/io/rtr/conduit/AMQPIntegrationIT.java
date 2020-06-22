package io.rtr.conduit;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.SocketUtil;
import io.rtr.conduit.amqp.consumer.Consumer;
import io.rtr.conduit.amqp.impl.AMQPConsumerBuilder;
import io.rtr.conduit.amqp.impl.AMQPPublisherBuilder;
import io.rtr.conduit.amqp.publisher.Publisher;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static io.rtr.conduit.amqp.SystemPropertiesUtil.withSystemProperty;
import static java.lang.Thread.sleep;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class AMQPIntegrationIT {
    private static final String TRUST_STORE = Paths.get(
        System.getProperty("user.dir"),
        "target",
        "test-classes",
        "keystore.jks"
    ).toString();

    private static final String EXCHANGE = "foo";
    private static final String QUEUE = "queue";
    private static final String ROUTING_KEY = "bar";

    private static int PORT;
    private static final Broker BROKER;

    private AMQPConsumerCallback callback;

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

    private void connectAndPublish(Publisher publisher, Consumer consumer, AMQPMessageBundle message) {
        try {
            publisher.connect();
            consumer.connect();
            consumer.listen();

            publisher.publish(message);

            // Sleep for consistency
            sleep(500);
        } catch (IOException | TimeoutException | InterruptedException e) {
            // gotta catch 'em all!
            throw new AssertionError("Publishing threw an exception", e);
        }
    }

    private static Consumer buildConsumer(AMQPConsumerCallback callback) {
        return AMQPConsumerBuilder.synchronous()
                .ssl(true)
                .host("localhost")
                .port(PORT)
                .virtualHost("local")
                .username("guest")
                .password("guest")
                .exchange("foo")
                .autoCreateAndBind(
                        EXCHANGE,
                        AMQPConsumerBuilder.ExchangeType.DIRECT,
                        QUEUE,
                        ROUTING_KEY
                )
                .callback(callback)
                .build();
    }

    private static Publisher buildPublisher() {
        return AMQPPublisherBuilder.builder()
                .ssl(true)
                .host("localhost")
                .port(PORT)
                .virtualHost("local")
                .username("guest")
                .password("guest")
                .exchange(EXCHANGE)
                .routingKey(ROUTING_KEY)
                .build();
    }

    @Test
    public void testSslAmqpTransport() {
        callback = mock(AMQPConsumerCallback.class);
        AMQPMessageBundle message = new AMQPMessageBundle("a message");
        withSystemProperty("javax.net.ssl.trustStore", TRUST_STORE, () -> {
            connectAndPublish(buildPublisher(), buildConsumer(callback), message);

            Mockito.verify(callback, times(1)).handle(any());
        });
    }
}
