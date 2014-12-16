package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.AbstractAMQPTransport;
import io.rtr.conduit.amqp.transport.TransportConnectionProperties;
import io.rtr.conduit.amqp.transport.TransportListenProperties;
import io.rtr.conduit.amqp.transport.TransportMessageBundle;
import io.rtr.conduit.amqp.transport.TransportPublishProperties;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Currently, programmatic creation of exchanges and queues is disallowed and discouraged.
 */
public class AMQPTransport extends AbstractAMQPTransport {
    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection;
    private Channel channel;
    private final static String POISON = ".poison";

    AMQPTransport(String host, int port) {
        factory.setHost(host);
        factory.setPort(port);
    }

    protected Channel getChannel() {
        return channel;
    }

    @Override
    protected void connectImpl(TransportConnectionProperties properties) throws IOException {
        AMQPConnectionProperties connectionProperties = (AMQPConnectionProperties)properties;

        factory.setUsername(connectionProperties.getUsername());
        factory.setPassword(connectionProperties.getPassword());
        factory.setVirtualHost(connectionProperties.getVirtualHost());
        factory.setConnectionTimeout(connectionProperties.getConnectionTimeout());
        factory.setRequestedHeartbeat(connectionProperties.getHeartbeatInterval());

        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(1);
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

    @Override
    protected void stopImpl() throws IOException {
        //! As with closing the connection, closing an already
        //  closed channel is considered success.
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (AlreadyClosedException ignored) {}
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
        this.connection = connection;
    }
}
