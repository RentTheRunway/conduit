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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.rtr.conduit.amqp.SystemPropertiesUtil.withSystemProperty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;

public class AMQPIntegrationIT {
    private static final String TRUST_STORE =
            Paths.get(System.getProperty("user.dir"), "target", "test-classes", "keystore.jks")
                    .toString();

    private static final String EXCHANGE = "foo";
    private static final String QUEUE = "queue";
    private static final String ROUTING_KEY = "bar";

    private static final int PORT;
    private static final BrokerOptions OPTIONS;
    private static final Broker BROKER;

    static {
        BROKER = new Broker();
        PORT = SocketUtil.getAvailablePort();
        OPTIONS = new BrokerOptions();
        OPTIONS.setConfigProperty("qpid.amqp_port", String.valueOf(PORT));
        OPTIONS.setConfigProperty(
                "qpid.broker.defaultPreferenceStoreAttributes", "{\"type\": \"Noop\"}");
        OPTIONS.setConfigProperty("qpid.vhost", "local");
        OPTIONS.setConfigProperty("qpid.keystore", TRUST_STORE);
        OPTIONS.setConfigurationStoreType("Memory");
        OPTIONS.setStartupLoggedToSystemOut(true);
    }

    @BeforeClass
    public static void fixtureSetUp() throws Exception {
        BROKER.startup(OPTIONS);
    }

    @AfterClass
    public static void fixtureTearDown() {
        BROKER.shutdown();
    }

    private void connectResources(Publisher publisher, Consumer consumer) {
        try {
            publisher.connect();
            consumer.connect();
            consumer.listen();

            assertTrue(publisher.isConnected());
            assertTrue(consumer.isConnected());
        } catch (IOException e) {
            // gotta catch 'em all!
            throw new AssertionError("Publishing threw an IOException", e);
        }
    }

    private void publishMessage(Publisher publisher, AMQPMessageBundle message) {
        try {
            publisher.publish(message);
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
                .autoCreateAndBind(EXCHANGE, AMQPConsumerBuilder.ExchangeType.DIRECT, QUEUE, ROUTING_KEY)
                .automaticRecoveryEnabled(true)
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
                .automaticRecoveryEnabled(true)
                .build();
    }

    @Test
    public void testSslAmqpTransport() {
        AMQPConsumerCallback callback = mock(AMQPConsumerCallback.class);
        AMQPMessageBundle message = new AMQPMessageBundle("a message");
        withSystemProperty(
                "javax.net.ssl.trustStore",
                TRUST_STORE,
                () -> {
                    Publisher publisher = buildPublisher();
                    Consumer consumer = buildConsumer(callback);
                    connectResources(publisher, consumer);
                    publishMessage(publisher, message);

                    Mockito.verify(callback, timeout(500).times(1)).handle(any());

                    try {
                        consumer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test
    public void testReconnectAfterBrokerShutdown() {
        AMQPConsumerCallback callback = mock(AMQPConsumerCallback.class);
        AMQPMessageBundle message = new AMQPMessageBundle("a message");
        withSystemProperty(
                "javax.net.ssl.trustStore",
                TRUST_STORE,
                () -> {
                    Publisher publisher = buildPublisher();
                    Consumer consumer = buildConsumer(callback);
                    connectResources(publisher, consumer);

                    BROKER.shutdown();
                    assertFalse(publisher.isConnected());
                    assertFalse(consumer.isConnected());
                    Mockito.verify(callback).notifyOfShutdown(any(), any());

                    try {
                        BROKER.startup(OPTIONS);
                        assertTrue(resourcesHaveConnected(publisher, consumer));
                    } catch (Exception e) {
                        throw new AssertionError("Failed to restart the broker", e);
                    }

                    publishMessage(publisher, message);
                    Mockito.verify(callback, timeout(2000).times(1)).handle(any());

                    try {
                        consumer.close();
                    } catch (IOException ignored) {
                    }
                });
    }

    private boolean resourcesHaveConnected(Publisher publisher, Consumer consumer) {
        final CountDownLatch done = new CountDownLatch(1);

        new Thread(
                () -> {
                    while (true) {
                        try {
                            if ((publisher == null || publisher.isConnected())
                                    && (consumer == null || consumer.isConnected())) {
                                done.countDown();
                            }
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException ignored) {
                        }
                    }
                })
                .start();

        try {
            return done.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }
}
