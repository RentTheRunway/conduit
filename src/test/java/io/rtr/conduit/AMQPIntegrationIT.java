package io.rtr.conduit;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.SocketUtil;
import io.rtr.conduit.amqp.consumer.Consumer;
import io.rtr.conduit.amqp.impl.AMQPConnection;
import io.rtr.conduit.amqp.impl.AMQPConnectionProperties;
import io.rtr.conduit.amqp.impl.AMQPConsumerBuilder;
import io.rtr.conduit.amqp.impl.AMQPPublisherBuilder;
import io.rtr.conduit.amqp.publisher.Publisher;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.junit.AfterClass;
import org.junit.Assert;
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

    private static AMQPConnection buildSslConnection() throws IOException, TimeoutException {
        AMQPConnection conn = new AMQPConnection(true, "localhost", PORT, null);
        AMQPConnectionProperties props = AMQPConnectionProperties.builder()
                .username("guest")
                .password("guest")
                .automaticRecoveryEnabled(true)
                .virtualHost("local")
                .build();
        conn.connect(props);
        return conn;
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

    private static Consumer buildConsumerWithSharedConnection(AMQPConnection connection, AMQPConsumerCallback callback) {
        return AMQPConsumerBuilder.synchronous()
                .exchange("foo")
                .autoCreateAndBind(EXCHANGE, AMQPConsumerBuilder.ExchangeType.DIRECT, QUEUE, ROUTING_KEY)
                .automaticRecoveryEnabled(true)
                .callback(callback)
                .sharedConnection(connection)
                .build();
    }

    private static Consumer buildConsumerWithAutoDeleteQueue(AMQPConsumerCallback callback) {
        return AMQPConsumerBuilder.synchronous()
            .ssl(true)
            .host("localhost")
            .port(PORT)
            .virtualHost("local")
            .username("guest")
            .password("guest")
            .isAutoDeleteQueue(true)
            .autoCreateAndBind(EXCHANGE, AMQPConsumerBuilder.ExchangeType.DIRECT, "auto-delete-queue", ROUTING_KEY)
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

    private static Publisher buildPublisherWithSharedConnection(AMQPConnection connection) {
        return AMQPPublisherBuilder.builder()
                .exchange(EXCHANGE)
                .routingKey(ROUTING_KEY)
                .automaticRecoveryEnabled(true)
                .sharedConnection(connection)
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
    public void testAmqpTransportWithSharedConnection() {
        AMQPConsumerCallback callback = mock(AMQPConsumerCallback.class);
        AMQPMessageBundle message = new AMQPMessageBundle("a message");
        withSystemProperty(
                "javax.net.ssl.trustStore",
                TRUST_STORE,
                () -> {

                    AMQPConnection connection ;
                    try {
                        connection = buildSslConnection();
                    } catch (IOException | TimeoutException e) {
                        throw new IllegalStateException(e);
                    }
                    Consumer consumer = null;
                    try {
                        consumer = buildConsumerWithSharedConnection(connection, callback);
                        Publisher publisher = buildPublisherWithSharedConnection(connection);
                        connectResources(publisher, consumer);
                        publishMessage(publisher, message);

                        Mockito.verify(callback, timeout(500).times(1)).handle(any());
                    }
                    finally {
                        try {
                            if (consumer != null) {
                                consumer.close();
                            }
                            connection.disconnect();
                        } catch (IOException e) {
                            Assert.fail(String.format("Error disconnecting consumer channel or broker: %s", e));
                        }
                    }
                });
    }

    @Test
    public void testManualReconnectAfterManualClose() {
        AMQPConsumerCallback callback = mock(AMQPConsumerCallback.class);
        AMQPMessageBundle message = new AMQPMessageBundle("a message");
        withSystemProperty(
                "javax.net.ssl.trustStore",
                TRUST_STORE,
                () -> {
                    Publisher publisher = buildPublisher();
                    Consumer consumer = buildConsumer(callback);
                    connectResources(publisher, consumer);

                    try {
                        publisher.close();
                        consumer.close();
                    } catch (IOException e) {
                        Assert.fail(String.format("Error disconnecting publisher or consumer: %s", e));
                    }

                    assertFalse(publisher.isConnected());
                    assertFalse(consumer.isConnected());

                    connectResources(publisher, consumer);

                    publishMessage(publisher, message);
                    Mockito.verify(callback, timeout(2000).times(1)).handle(any());

                    try {
                        consumer.close();
                        publisher.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    @Test
    public void testAmqpTransportWithAutoDeleteQueue() {
        AMQPConsumerCallback callback = mock(AMQPConsumerCallback.class);
        AMQPMessageBundle message = new AMQPMessageBundle("a message");
        withSystemProperty(
            "javax.net.ssl.trustStore",
            TRUST_STORE,
            () -> {

                AMQPConnection connection ;
                try {
                    connection = buildSslConnection();
                } catch (IOException | TimeoutException e) {
                    throw new IllegalStateException(e);
                }
                Consumer consumer = null;
                try {
                    consumer = buildConsumerWithAutoDeleteQueue(callback);
                    Publisher publisher = buildPublisher();
                    connectResources(publisher, consumer);
                    publishMessage(publisher, message);

                    Mockito.verify(callback, timeout(500).times(1)).handle(any());
                }
                finally {
                    try {
                        if (consumer != null) {
                            consumer.close();
                        }
                        connection.disconnect();
                    } catch (IOException e) {
                        Assert.fail(String.format("Error disconnecting consumer channel or broker: %s", e));
                    }
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
