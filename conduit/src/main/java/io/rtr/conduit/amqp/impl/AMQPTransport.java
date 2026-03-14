package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MetricsCollector;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.AbstractAMQPTransport;
import io.rtr.conduit.amqp.transport.TransportConnectionProperties;
import io.rtr.conduit.amqp.transport.TransportListenProperties;
import io.rtr.conduit.amqp.transport.TransportMessageBundle;
import io.rtr.conduit.amqp.transport.TransportPublishProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class AMQPTransport extends AbstractAMQPTransport {

    class DynamicQueueCleanupShutdownListener implements ShutdownListener {
        // @VisibleForTesting
        CompletableFuture<Void> queueCleanupJob;

        @Override
        public void shutdownCompleted(final ShutdownSignalException e) {
            // Shutdown handlers run directly in the AMQP Connection's worker loop.
            // As such, trying to initiate new AMQP commands from within it creates a deadlock.
            queueCleanupJob =
                    CompletableFuture.runAsync(
                            () -> {
                                if (dynamicQueue != null) {
                                    this.deleteQueueOnSeparateChannel(dynamicQueue);
                                    LOGGER.debug("Deleted dynamic queue '{}'.", dynamicQueue);
                                }
                                if (dynamicPoisonQueueInUse) {
                                    this.deleteQueueOnSeparateChannel(POISON + "." + dynamicQueue);
                                    LOGGER.debug(
                                            "Deleted poison message queue for dynamic queue '{}'.",
                                            dynamicQueue);
                                }
                            });
        }

        private void deleteQueueOnSeparateChannel(final String queue) {
            try (final Channel cleanupChannel = conn.createChannel()) {
                cleanupChannel.queueDelete(queue);
            } catch (final TimeoutException | IOException | RuntimeException ex) {
                LOGGER.error(
                        "Failed to delete conduit managed queue '{}', this could cause a queue to leak on the broker! Proceeding with closing channel.",
                        queue,
                        ex);
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPTransport.class);
    private AMQPConnection conn;
    private final boolean hasPrivateConnection;
    private Channel channel;
    static final String POISON = ".poison";
    private String dynamicQueue;
    private boolean dynamicPoisonQueueInUse = false;

    protected AMQPTransport(
            final boolean ssl,
            final String host,
            final int port,
            final MetricsCollector metricsCollector) {
        this(new AMQPConnection(ssl, host, port, metricsCollector), true);
    }

    protected AMQPTransport(final AMQPConnection sharedConnection) {
        this(sharedConnection, false);
    }

    private AMQPTransport(final AMQPConnection connection, final boolean hasPrivateConnection) {
        conn = connection;
        this.hasPrivateConnection = hasPrivateConnection;
    }

    protected Channel getChannel() {
        return channel;
    }

    @Override
    protected boolean isConnectedImpl() {
        return this.conn.isConnected();
    }

    @Override
    protected void connectImpl(final TransportConnectionProperties properties) throws IOException {
        if (hasPrivateConnection && !this.isConnected()) {
            try {
                conn.connect((AMQPConnectionProperties) properties);
            } catch (final TimeoutException e) {
                throw new IOException("Timed-out waiting for new connection", e);
            }
        }

        if (channel == null || !channel.isOpen()) {
            channel = conn.createChannel();
        }
    }

    @Override
    protected void closeImpl() throws IOException {
        if (hasPrivateConnection) {
            if (conn.isConnected()) {
                conn.disconnect();
            }
        }
        // If the connection is shared, just try and close the channel
        else this.stop();
    }

    @Override
    protected AMQPQueueConsumer getConsumer(
            final Object callback,
            final AMQPCommonListenProperties commonListenProperties,
            final String poisonPrefix) {
        return new AMQPQueueConsumer(
                this.getChannel(),
                (AMQPConsumerCallback) callback,
                commonListenProperties.getThreshold(),
                poisonPrefix,
                commonListenProperties.isPoisonQueueEnabled());
    }

    @Override
    protected AMQPCommonListenProperties getCommonListenProperties(
            final TransportListenProperties properties) {
        return (AMQPListenProperties) properties;
    }

    @Override
    protected Object getConsumerCallback(final TransportListenProperties properties) {
        return ((AMQPListenProperties) properties).getCallback();
    }

    @Override
    protected void listenImpl(final TransportListenProperties properties) throws IOException {
        final boolean noAutoAck = false;
        final AMQPCommonListenProperties commonListenProperties =
                this.getCommonListenProperties(properties);
        String queue = commonListenProperties.getQueue();
        String poisonPrefix = commonListenProperties.getPoisonPrefix();
        final boolean exclusive = commonListenProperties.getExclusive();

        if (commonListenProperties.isDynamicQueueCreation()) {
            queue =
                    this.createDynamicQueue(
                            commonListenProperties.getExchange(),
                            commonListenProperties.getDynamicQueueRoutingKey(),
                            commonListenProperties.isPoisonQueueEnabled());
            poisonPrefix = "." + queue;
        } else if (commonListenProperties.isAutoCreateAndBind()) {
            this.autoCreateAndBind(
                    commonListenProperties.getExchange(),
                    commonListenProperties.getExchangeType(),
                    commonListenProperties.getQueue(),
                    commonListenProperties.isAutoDeleteQueue(),
                    commonListenProperties.isPoisonQueueEnabled(),
                    commonListenProperties.getRoutingKey());
        }

        if (commonListenProperties.shouldPurgeOnConnect()) {
            channel.queuePurge(queue);
        }

        final AMQPQueueConsumer consumer =
                this.getConsumer(
                        this.getConsumerCallback(properties), commonListenProperties, poisonPrefix);
        this.getChannel().basicQos(commonListenProperties.getPrefetchCount());
        this.getChannel()
                .basicConsume(queue, noAutoAck, "", false, exclusive, (Map) null, consumer);
    }

    protected String createDynamicQueue(
            final String exchange, final String routingKey, final boolean isPoisonQueueEnabled)
            throws IOException {

        channel.addShutdownListener(new DynamicQueueCleanupShutdownListener());

        if (dynamicQueue == null) {
            LOGGER.debug("Creating new dynamic queue.");
            dynamicQueue = channel.queueDeclare().getQueue();
        } else {
            channel.queueDeclare(dynamicQueue, false, true, true, null);
        }

        LOGGER.debug("Declared dynamic queue '{}'.", dynamicQueue);
        channel.queueBind(dynamicQueue, exchange, routingKey);
        LOGGER.debug(
                "Bound dynamic queue '{}' to '{}' using routing key '{}'.",
                dynamicQueue,
                exchange,
                routingKey);

        if (isPoisonQueueEnabled) {
            final String poisonQueue = POISON + "." + dynamicQueue;
            final Map<String, Object> settings = new HashMap<>();
            channel.queueDeclare(poisonQueue, false, true, true, settings);
            dynamicPoisonQueueInUse = true;
            channel.queueBind(poisonQueue, exchange, routingKey + "." + dynamicQueue + POISON);
        }

        return dynamicQueue;
    }

    void autoCreateAndBind(
            final String exchange,
            final String exchangeType,
            final String queue,
            final boolean isAutoDeleteQueue,
            final boolean isPoisonQueueEnabled,
            final String routingKey)
            throws IOException {
        channel.exchangeDeclare(exchange, exchangeType, true);
        channel.queueDeclare(queue, !isAutoDeleteQueue, false, isAutoDeleteQueue, null);
        channel.queueBind(queue, exchange, routingKey);
        if (isPoisonQueueEnabled) {
            final String poisonQueue = queue + POISON;
            channel.queueDeclare(poisonQueue, !isAutoDeleteQueue, false, isAutoDeleteQueue, null);
            channel.queueBind(poisonQueue, exchange, routingKey + POISON);
        }
    }

    @Override
    protected void stopImpl() throws IOException {
        // As with closing the connection, closing an already
        // closed channel is considered success.
        if (channel != null && channel.isOpen()) {

            try {
                channel.close();
            } catch (final TimeoutException e) {
                throw new IOException("Timed-out closing connection", e);
            } catch (final AlreadyClosedException ignored) {
            }
        }
        if (hasPrivateConnection) {
            conn.stopListening();
        }
    }

    @Override
    protected boolean isStoppedImpl(final int waitForMillis) throws InterruptedException {
        if (hasPrivateConnection) {
            return conn.waitToStopListening(Duration.ofMillis(waitForMillis));
        } else {
            return (channel == null || !channel.isOpen());
        }
    }

    @Override
    protected boolean publishImpl(
            final TransportMessageBundle bundle, final TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        final AMQPPublishProperties publishProperties = (AMQPPublishProperties) properties;
        final AMQPMessageBundle messageBundle = (AMQPMessageBundle) bundle;

        if (publishProperties.isConfirmEnabled()) {
            channel.confirmSelect();
        }

        channel.basicPublish(
                publishProperties.getExchange(),
                publishProperties.getRoutingKey(),
                messageBundle.getBasicProperties(),
                messageBundle.getBody());

        if (publishProperties.isConfirmEnabled()) {
            return channel.waitForConfirms(publishProperties.getTimeout());
        }

        return true;
    }

    @Override
    protected <E> boolean transactionalPublishImpl(
            final Collection<E> messageBundles, final TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        channel.txSelect();

        boolean rollback = true;

        try {
            for (final E messageBundle : messageBundles) {
                if (!this.publishImpl((AMQPMessageBundle) messageBundle, properties)) return false;
            }
            rollback = false;
        } finally {
            // ! Explicitly roll back.
            if (rollback) channel.txRollback();
            else channel.txCommit();
        }

        return true;
    }

    // Package private for testing
    void setChannel(final Channel channel) {
        this.channel = channel;
    }

    // Package private for testing
    void setConnection(final AMQPConnection connection) {
        this.conn = connection;
    }
}
