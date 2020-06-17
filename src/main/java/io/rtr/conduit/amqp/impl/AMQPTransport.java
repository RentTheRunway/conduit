package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.AbstractAMQPTransport;
import io.rtr.conduit.amqp.connection.AMQPConnection;
import io.rtr.conduit.amqp.connection.RabbitAMQPConnection;
import io.rtr.conduit.amqp.connection.RabbitAMQPConnectionBuilder;
import io.rtr.conduit.amqp.transport.TransportConnectionProperties;
import io.rtr.conduit.amqp.transport.TransportListenProperties;
import io.rtr.conduit.amqp.transport.TransportMessageBundle;
import io.rtr.conduit.amqp.transport.TransportPublishProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class AMQPTransport extends AbstractAMQPTransport {
    private static final Logger log = LoggerFactory.getLogger(AMQPTransport.class);
    private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);
    private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName(String.format("AMQPConnection-%s", THREAD_COUNT.getAndIncrement()));
            return thread;
        }
    };
    private RabbitAMQPConnection rabbitAMQPConnection;
    private Channel channel;
    final static String POISON = ".poison";
    private boolean isConnectionChannelMultiplexingEnabled = false;
    private RabbitAMQPConnectionBuilder rabbitAMQPConnectionBuilder;
    private int connectionTimeout;
    private ConnectionFactory factory;

    AMQPTransport(boolean ssl, String host, int port) {
        this.rabbitAMQPConnectionBuilder = new RabbitAMQPConnectionBuilder();

        if (ssl) {
            //factory.setSocketFactory(SSLSocketFactory.getDefault());
            this.rabbitAMQPConnectionBuilder.setSocketFactory(SSLSocketFactory.getDefault());
        }

        this.rabbitAMQPConnectionBuilder.setHost(host);
        this.rabbitAMQPConnectionBuilder.setPort(port);
//        factory.setHost(host);
//        factory.setPort(port);
    }

    protected Channel getChannel() {
        if(this.channel == null && this.isConnectionChannelMultiplexingEnabled){
            log.warn("Connection-channel multiplexing has been enabled, transport does not have a channel.");
        }
        return channel;
    }

    @Override
    protected void connectImpl(TransportConnectionProperties properties) throws IOException {
        AMQPConnectionProperties connectionProperties = (AMQPConnectionProperties)properties;
        this.rabbitAMQPConnectionBuilder.setThreadFactory(DAEMON_THREAD_FACTORY);
        this.rabbitAMQPConnectionBuilder.setUsername(connectionProperties.getUsername());
        this.rabbitAMQPConnectionBuilder.setPassword(connectionProperties.getPassword());
        this.rabbitAMQPConnectionBuilder.setVirtualHost(connectionProperties.getVirtualHost());
        this.connectionTimeout = connectionProperties.getConnectionTimeout();
        this.rabbitAMQPConnectionBuilder.setConnectionTimeout(connectionProperties.getConnectionTimeout());
        this.rabbitAMQPConnectionBuilder.setRequestedHeartbeat(connectionProperties.getHeartbeatInterval());
        this.rabbitAMQPConnectionBuilder.setAutomaticRecoveryEnabled(connectionProperties.isAutomaticRecoveryEnabled());

//        factory.setThreadFactory(DAEMON_THREAD_FACTORY);
//        factory.setUsername(connectionProperties.getUsername());
        //factory.setPassword(connectionProperties.getPassword());
        //factory.setVirtualHost(connectionProperties.getVirtualHost());
        //factory.setConnectionTimeout(connectionProperties.getConnectionTimeout());
        //factory.setRequestedHeartbeat(connectionProperties.getHeartbeatInterval());
        //factory.setAutomaticRecoveryEnabled(connectionProperties.isAutomaticRecoveryEnabled());
        this.isConnectionChannelMultiplexingEnabled = connectionProperties.isConnectionChannelMultiplexingEnabled();

        try {
            this.rabbitAMQPConnection = rabbitAMQPConnectionBuilder.build();
            this.rabbitAMQPConnection.connect();
        } catch (TimeoutException e) {
            throw new IOException("Timed-out waiting for new connection", e);
        }

        // for backwards compatibility
        if(!this.isConnectionChannelMultiplexingEnabled){
            channel = rabbitAMQPConnection.getConnection().createChannel();
            channel.basicQos(1);
        }
    }

    @Override
    protected void closeImpl() throws IOException {
        //! We are going to assume that closing an already closed
        //  connection is considered success.
        if (rabbitAMQPConnection != null && rabbitAMQPConnection.getConnection().isOpen()) {
            try {
                rabbitAMQPConnection.getConnection().close(this.connectionTimeout);
            } catch (AlreadyClosedException ignored) {}
        }
    }

    @Override
    protected AMQPQueueConsumer getConsumer(Object callback, AMQPCommonListenProperties commonListenProperties, String poisonPrefix){
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
        return (AMQPListenProperties)properties;
    }

    @Override
    protected Object getConsumerCallback(TransportListenProperties properties) {
        return ((AMQPListenProperties)properties).getCallback();
    }

    @Override
    protected AMQPConnection getConnection() {
        return null;
    }

    @Override
    protected void listenImpl(TransportListenProperties properties) throws IOException {
        final boolean noAutoAck = false;
        AMQPCommonListenProperties commonListenProperties = getCommonListenProperties(properties);
        String queue = commonListenProperties.getQueue();
        String poisonPrefix = commonListenProperties.getPoisonPrefix();

        if(commonListenProperties.isDynamicQueueCreation()) {
            queue = createDynamicQueue(commonListenProperties.getExchange(),
                    commonListenProperties.getDynamicQueueRoutingKey(),
                    commonListenProperties.isPoisonQueueEnabled());
            poisonPrefix = "." + queue;
        } else if(commonListenProperties.isAutoCreateAndBind()) {
            autoCreateAndBind(
                    commonListenProperties.getExchange(),
                    commonListenProperties.getExchangeType(),
                    commonListenProperties.getQueue(),
                    commonListenProperties.getRoutingKey(),
                    commonListenProperties.isPoisonQueueEnabled());
        }

        if(commonListenProperties.shouldPurgeOnConnect()){
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
        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, exchange, routingKey);
        if(isPoisonQueueEnabled){
            String poisonQueue = POISON + "." + queue;
            Map<String, Object> settings = new HashMap<String, Object>();
            channel.queueDeclare(poisonQueue, true, true, true, settings);
            channel.queueBind(poisonQueue, exchange, routingKey + "." + queue + POISON);
        }
        return queue;
    }

    void autoCreateAndBind(String exchange, String exchangeType, String queue, String routingKey, boolean isPoisonQueueEnabled) throws IOException {
        // Creates durable non-autodeleted exchange and queue(s).
        channel.exchangeDeclare(exchange, exchangeType, true);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, routingKey);
        if(isPoisonQueueEnabled){
            String poisonQueue = queue + POISON;
            channel.queueDeclare(poisonQueue, true, false, false, null);
            channel.queueBind(poisonQueue, exchange, routingKey + POISON);
        }

    }

    @Override
    protected void stopImpl() throws IOException {
        //! As with closing the connection, closing an already
        //  closed channel is considered success.
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (TimeoutException e) {
                throw new IOException("Timed-out closing connection", e);
            } catch (AlreadyClosedException ignored) {
            }
        }
    }

    @Override
    protected boolean publishImpl(TransportMessageBundle bundle, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        AMQPPublishProperties publishProperties = (AMQPPublishProperties)properties;
        AMQPMessageBundle messageBundle = (AMQPMessageBundle)bundle;

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
                if (!publishImpl((AMQPMessageBundle)messageBundle, properties))
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

        return !rollback;
    }

    //Package private for testing
    void setChannel(Channel channel) {
        this.channel = channel;
    }

    //Package private for testing
    void setConnectionFactory(ConnectionFactory factory){
        this.factory = factory;
    }

    //Package private for testing
    void setConnection(Connection connection){
        this.rabbitAMQPConnection.setConnection(connection);
    }
}
