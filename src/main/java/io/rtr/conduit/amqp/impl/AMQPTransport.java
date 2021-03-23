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
        //@VisibleForTesting
        CompletableFuture<Void> queueCleanupJob;

        @Override
        public void shutdownCompleted(ShutdownSignalException e) {
            // Shutdown handlers run directly in the AMQP Connection's worker loop.
            // As such, trying to initiate new AMQP commands from within it creates a deadlock.
            queueCleanupJob = CompletableFuture.runAsync(()-> {
                if (dynamicQueue != null) {
                    deleteQueueOnSeparateChannel(dynamicQueue);
                    LOGGER.debug("Deleted dynamic queue '{}'.", dynamicQueue);
                }
                if (dynamicPoisonQueueInUse) {
                    deleteQueueOnSeparateChannel(POISON + "." + dynamicQueue);
                    LOGGER.debug("Deleted poison message queue for dynamic queue '{}'.", dynamicQueue);
                }
            });
        }

        private void deleteQueueOnSeparateChannel(String queue) {
            try (Channel cleanupChannel = conn.createChannel()) {
                cleanupChannel.queueDelete(queue);
            } catch (TimeoutException | IOException | RuntimeException ex) {
                LOGGER.error("Failed to delete conduit managed queue '{}', this could cause a queue to leak on the broker! Proceeding with closing channel.", queue, ex);
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPTransport.class);
    private AMQPConnection conn;
    private boolean hasPrivateConnection;
    private Channel channel;
    static final String POISON = ".poison";
    private String dynamicQueue;
    private boolean dynamicPoisonQueueInUse = false;

    protected AMQPTransport(boolean ssl, String host, int port, MetricsCollector metricsCollector) {
        this(new AMQPConnection(ssl, host, port, metricsCollector), true);
    }

    protected AMQPTransport(AMQPConnection sharedConnection) {
        this(sharedConnection, false);
    }

    private AMQPTransport(AMQPConnection connection, boolean hasPrivateConnection) {
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
    protected void connectImpl(TransportConnectionProperties properties) throws IOException {
        if (hasPrivateConnection && !isConnected()) {
            try {
                conn.connect((AMQPConnectionProperties) properties);
            } catch (TimeoutException e) {
                throw new IOException("Timed-out waiting for new connection", e);
            }

        }
        if (channel == null) {
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
        //If the connection is shared, just try and close the channel
        else stop();

    }

    @Override
    protected AMQPQueueConsumer getConsumer(Object callback, AMQPCommonListenProperties commonListenProperties, String poisonPrefix) {
        return new AMQPQueueConsumer(
                getChannel(),
                (AMQPConsumerCallback) callback,
                commonListenProperties.getThreshold(),
                poisonPrefix,
                commonListenProperties.isPoisonQueueEnabled()
        );
    }

    @Override
    protected AMQPCommonListenProperties getCommonListenProperties(TransportListenProperties properties) {
        return (AMQPListenProperties) properties;
    }

    @Override
    protected Object getConsumerCallback(TransportListenProperties properties) {
        return ((AMQPListenProperties) properties).getCallback();
    }

    @Override
    protected void listenImpl(TransportListenProperties properties) throws IOException {
        final boolean noAutoAck = false;
        AMQPCommonListenProperties commonListenProperties = getCommonListenProperties(properties);
        String queue = commonListenProperties.getQueue();
        String poisonPrefix = commonListenProperties.getPoisonPrefix();

        if (commonListenProperties.isDynamicQueueCreation()) {
            queue = createDynamicQueue(commonListenProperties.getExchange(),
                    commonListenProperties.getDynamicQueueRoutingKey(),
                    commonListenProperties.isPoisonQueueEnabled());
            poisonPrefix = "." + queue;
        } else if (commonListenProperties.isAutoCreateAndBind()) {
            autoCreateAndBind(
                    commonListenProperties.getExchange(),
                    commonListenProperties.getExchangeType(),
                    commonListenProperties.getQueue(),
                    commonListenProperties.getRoutingKey(),
                    commonListenProperties.isPoisonQueueEnabled());
        }

        if (commonListenProperties.shouldPurgeOnConnect()) {
            channel.queuePurge(queue);
        }

        AMQPQueueConsumer consumer = getConsumer(
                getConsumerCallback(properties),
                commonListenProperties,
                poisonPrefix);
        getChannel().basicQos(commonListenProperties.getPrefetchCount());
        getChannel().basicConsume(queue, noAutoAck, consumer);
    }

    protected String createDynamicQueue(String exchange,
                                        String routingKey,
                                        boolean isPoisonQueueEnabled) throws IOException {

        channel.addShutdownListener(new DynamicQueueCleanupShutdownListener());

        if (dynamicQueue == null) {
            LOGGER.debug("Creating new dynamic queue.");
            dynamicQueue = channel.queueDeclare().getQueue();
        } else {
            channel.queueDeclare(dynamicQueue, false, true, true, null);
        }

        LOGGER.debug("Declared dynamic queue '{}'.", dynamicQueue);
        channel.queueBind(dynamicQueue, exchange, routingKey);
        LOGGER.debug("Bound dynamic queue '{}' to '{}' using routing key '{}'.", dynamicQueue, exchange, routingKey);

        if (isPoisonQueueEnabled) {
            String poisonQueue = POISON + "." + dynamicQueue;
            Map<String, Object> settings = new HashMap<>();
            channel.queueDeclare(poisonQueue, false, true, true, settings);
            dynamicPoisonQueueInUse = true;
            channel.queueBind(poisonQueue, exchange, routingKey + "." + dynamicQueue + POISON);
        }

        return dynamicQueue;
    }

    void autoCreateAndBind(String exchange, String exchangeType, String queue, String routingKey, boolean isPoisonQueueEnabled) throws IOException {
        // Creates durable non-autodeleted exchange and queue(s).
        channel.exchangeDeclare(exchange, exchangeType, true);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, routingKey);
        if (isPoisonQueueEnabled) {
            String poisonQueue = queue + POISON;
            channel.queueDeclare(poisonQueue, true, false, false, null);
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
            } catch (TimeoutException e) {
                throw new IOException("Timed-out closing connection", e);
            } catch (AlreadyClosedException ignored) {
            }
        }
        if (hasPrivateConnection) {
            conn.stopListening();
        }
    }

    @Override
    protected boolean isStoppedImpl(int waitForMillis) throws InterruptedException {
        if (hasPrivateConnection) {
            return conn.waitToStopListening(Duration.ofMillis(waitForMillis));
        } else {
            return (channel == null || !channel.isOpen());
        }
    }

    @Override
    protected boolean publishImpl(TransportMessageBundle bundle, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        AMQPPublishProperties publishProperties = (AMQPPublishProperties) properties;
        AMQPMessageBundle messageBundle = (AMQPMessageBundle) bundle;

        if (publishProperties.isConfirmEnabled()) {
            channel.confirmSelect();
        }

        channel.basicPublish(
                publishProperties.getExchange()
                , publishProperties.getRoutingKey()
                , messageBundle.getBasicProperties()
                , messageBundle.getBody()
        );

        if (publishProperties.isConfirmEnabled()) {
            return channel.waitForConfirms(publishProperties.getTimeout());
        }

        return true;
    }

    @Override
    protected <E> boolean transactionalPublishImpl(Collection<E> messageBundles, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        channel.txSelect();

        boolean rollback = true;

        try {
            for (E messageBundle : messageBundles) {
                if (!publishImpl((AMQPMessageBundle) messageBundle, properties))
                    return false;
            }
            rollback = false;
        } finally {
            //! Explicitly roll back.
            if (rollback)
                channel.txRollback();
            else
                channel.txCommit();
        }

        return true;
    }

    //Package private for testing
    void setChannel(Channel channel) {
        this.channel = channel;
    }

    //Package private for testing
    void setConnection(AMQPConnection connection) {
        this.conn = connection;
    }
}
