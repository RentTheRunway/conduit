package conduit.amqp;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import conduit.amqp.consumer.AMQPQueueConsumer;
import conduit.amqp.consumer.AMQPQueueConsumerFactory;
import conduit.transport.Transport;
import conduit.transport.TransportConnectionProperties;
import conduit.transport.TransportListenProperties;
import conduit.transport.TransportMessageBundle;
import conduit.transport.TransportPublishProperties;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

/**
 * Currently, programmatic creation of exchanges and queues is disallowed and discouraged.
 */
public class AMQPTransport extends Transport {
    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection;
    private Channel channel;
    private AMQPQueueConsumerFactory amqpQueueConsumerFactory;

    public AMQPTransport(String host, int port) {
        factory.setHost(host);
        factory.setPort(port);
        amqpQueueConsumerFactory = new AMQPQueueConsumerFactory();
    }

    public void setAmqpQueueConsumerFactory(AMQPQueueConsumerFactory amqpQueueConsumerFactory) {
        this.amqpQueueConsumerFactory = amqpQueueConsumerFactory;
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
                connection.close();
            } catch (AlreadyClosedException ignored) {}
        }
    }

    @Override
    protected void listenImpl(TransportListenProperties properties) throws IOException {
        final boolean noAutoAck = false;

        AMQPListenProperties listenProperties = (AMQPListenProperties)properties;
        AMQPQueueConsumer consumer = new AMQPQueueConsumer(
                channel
              , listenProperties.getCallback()
              , listenProperties.getThreshold()
        );

        if(listenProperties.isDrainOnListen()){
            channel.queuePurge(listenProperties.getQueue());
        }

        channel.basicConsume(listenProperties.getQueue(), noAutoAck, consumer);
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

        channel.basicPublish(
                publishProperties.getExchange()
              , publishProperties.getRoutingKey()
              , messageBundle.getBasicProperties()
              , messageBundle.getBody()
        );

        return channel.waitForConfirms(publishProperties.getTimeout());
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
}
