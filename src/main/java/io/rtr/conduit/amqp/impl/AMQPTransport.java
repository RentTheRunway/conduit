package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.AbstractAMQPTransport;
import io.rtr.conduit.amqp.transport.TransportConnectionProperties;
import io.rtr.conduit.amqp.transport.TransportExecutor;
import io.rtr.conduit.amqp.transport.TransportListenProperties;
import io.rtr.conduit.amqp.transport.TransportMessageBundle;
import io.rtr.conduit.amqp.transport.TransportPublishProperties;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AMQPTransport extends AbstractAMQPTransport {
    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection;
    private TransportExecutor executor;
    private Channel channel;
    static final String POISON = ".poison";

    AMQPTransport(boolean ssl, String host, int port) {
        if (ssl) {
            factory.setSocketFactory(SSLSocketFactory.getDefault());
        }

        factory.setHost(host);
        factory.setPort(port);
    }

    protected Channel getChannel() {
        return channel;
    }

    @Override
    protected boolean isConnectedImpl() {
        return this.connection != null && this.connection.isOpen();
    }

    @Override
    protected void connectImpl(TransportConnectionProperties properties) throws IOException {
        if (isConnected()) {
            return;
        }

        configureConnectionFactory((AMQPConnectionProperties) properties);
        try {
            initializeExecutor();
            connection = factory.newConnection(executor);
        } catch (TimeoutException e) {
            throw new IOException("Timed-out waiting for new connection", e);
        }

        channel = connection.createChannel();
        channel.basicQos(1);
    }

    private void configureConnectionFactory(AMQPConnectionProperties properties) {
        factory.setUsername(properties.getUsername());
        factory.setPassword(properties.getPassword());
        factory.setVirtualHost(properties.getVirtualHost());
        factory.setConnectionTimeout(properties.getConnectionTimeout());
        factory.setRequestedHeartbeat(properties.getHeartbeatInterval());
        factory.setAutomaticRecoveryEnabled(properties.isAutomaticRecoveryEnabled());
    }

    private void initializeExecutor() {
        if (executor != null) {
            executor.shutdown();
        }
        executor = new TransportExecutor();
    }

    @Override
    protected void closeImpl() throws IOException {
        //! We are going to assume that closing an already closed
        //  connection is considered success.
        if (connection != null && connection.isOpen()) {
            try {
                connection.close(factory.getConnectionTimeout());
            } catch (AlreadyClosedException ignored) {}
        }
        if (executor != null) {
            executor.shutdown();
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
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    protected boolean isStoppedImpl(int waitForMillis) throws InterruptedException {
        return executor.awaitTermination(waitForMillis, TimeUnit.MILLISECONDS);
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
        this.connection = connection;
    }
}
