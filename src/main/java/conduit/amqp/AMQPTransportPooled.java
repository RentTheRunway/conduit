package conduit.amqp;

import com.rabbitmq.client.*;
import conduit.amqp.consumer.AMQPQueueConsumer;
import conduit.amqp.consumer.AMQPQueueConsumerFactory;
import conduit.transport.*;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Currently, programmatic creation of exchanges and queues is disallowed and discouraged.
 *
 * Pool currently backed by http://commons.apache.org/proper/commons-pool/
 */
public class AMQPTransportPooled extends Transport {
    private ConnectionFactory factory = new ConnectionFactory();
    private PooledObjectFactory<Channel> poolableObjectFactory;
    private GenericObjectPool<Channel> genericObjectPool;
    private GenericObjectPoolConfig genericObjectPoolConfig;
    private Set<PooledObject<Channel>> channels;
    private AMQPQueueConsumerFactory amqpQueueConsumerFactory;

    public static class PoolExhaustedException extends Exception{};
    private AMQPTransportPooled(String host, int port, GenericObjectPoolConfig genericObjectPoolConfig) {
        this.factory.setHost(host);
        this.factory.setPort(port);
        this.genericObjectPoolConfig = genericObjectPoolConfig;
        this.channels = new HashSet<PooledObject<Channel>>();
        poolableObjectFactory = new PooledObjectFactory<Channel>() {
            @Override
            public PooledObject<Channel> makeObject() throws Exception {
                Connection connection = factory.newConnection();
                connection.addShutdownListener(new ShutdownListener() {
                    @Override
                    public void shutdownCompleted(ShutdownSignalException cause) {
                        System.out.println("SHutdown " + cause);
                    }
                });
                Channel channel = connection.createChannel();
                channel.basicQos(1);
                DefaultPooledObject defaultPooledObject = new DefaultPooledObject(channel);
                channels.add(defaultPooledObject);
                return defaultPooledObject;
            }

            @Override
            public void destroyObject(PooledObject<Channel> p) throws Exception {
                channels.remove(p);
                p.getObject().getConnection().close();
            }

            @Override
            public boolean validateObject(PooledObject<Channel> p) {
                return p.getObject().getConnection().isOpen();
            }

            @Override
            public void activateObject(PooledObject<Channel> p) throws Exception {}

            @Override
            public void passivateObject(PooledObject<Channel> p) throws Exception {}
        };

        genericObjectPool = new GenericObjectPool<Channel>(poolableObjectFactory, genericObjectPoolConfig);
    }

    public static class AMQPTransportPooledBuilder {
        private String host;
        private int port;
        private GenericObjectPoolConfig genericObjectPoolConfig;
        private AMQPQueueConsumerFactory amqpQueueConsumerFactory;

        public AMQPTransportPooledBuilder(){
            genericObjectPoolConfig = new GenericObjectPoolConfig();
        }

        public AMQPTransportPooledBuilder setHost(String host) {
            this.host = host;
            return this;
        }

        public AMQPTransportPooledBuilder setPort(int port) {
            this.port = port;
            return this;
        }

        public AMQPTransportPooledBuilder setMaxTotal(int maxTotal) {
            this.genericObjectPoolConfig.setMaxTotal(maxTotal);
            return this;
        }

        public AMQPTransportPooledBuilder setMinIdle(int minIdle) {
            this.genericObjectPoolConfig.setMinIdle(minIdle);
            return this;
        }

        public AMQPTransportPooledBuilder setMaxIdle(int maxIdle) {
            this.genericObjectPoolConfig.setMaxIdle(maxIdle);
            return this;
        }

        public AMQPTransportPooledBuilder setMaxWaitMillis(int maxWaitMillis) {
            this.genericObjectPoolConfig.setMaxWaitMillis(maxWaitMillis);
            return this;
        }

        public AMQPTransportPooledBuilder setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
            this.genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
            return this;
        }

        public AMQPTransportPooledBuilder setTestWhileIdle(boolean testWhileIdle) {
            this.genericObjectPoolConfig.setTestWhileIdle(testWhileIdle);
            return this;
        }

        public AMQPTransportPooledBuilder setBlockWhenExhausted(boolean blockWhenExhausted) {
            this.genericObjectPoolConfig.setBlockWhenExhausted(blockWhenExhausted);
            return this;
        }

        public AMQPTransportPooledBuilder setAmqpQueueConsumerFactory(AMQPQueueConsumerFactory amqpQueueConsumerFactory) {
            this.amqpQueueConsumerFactory = amqpQueueConsumerFactory;
            return this;
        }

        public AMQPTransportPooledBuilder setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
            this.genericObjectPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
            return this;
        }

        public AMQPTransportPooledBuilder setTestOnBorrow(boolean testOnBorrow) {
            this.genericObjectPoolConfig.setTestOnBorrow(testOnBorrow);
            return this;
        }

        public AMQPTransportPooled createAMQPTransportPooled() {
            AMQPTransportPooled amqpTransportPooled = new AMQPTransportPooled(host, port, genericObjectPoolConfig);

            if(amqpQueueConsumerFactory == null){
                amqpTransportPooled.amqpQueueConsumerFactory = new AMQPQueueConsumerFactory();
            }
            else{
                amqpTransportPooled.amqpQueueConsumerFactory = amqpQueueConsumerFactory;
            }
            return amqpTransportPooled;
        }
    }

    @Override
    protected void connectImpl(TransportConnectionProperties properties) throws IOException {
        AMQPConnectionProperties connectionProperties = (AMQPConnectionProperties)properties;
        factory.setUsername(connectionProperties.getUsername());
        factory.setPassword(connectionProperties.getPassword());
        factory.setVirtualHost(connectionProperties.getVirtualHost());
        factory.setConnectionTimeout(connectionProperties.getConnectionTimeout());
        factory.setRequestedHeartbeat(connectionProperties.getHeartbeatInterval());
    }

    @Override
    protected void closeImpl() throws IOException {
        //! We are going to assume that closing an already closed
        //  connection is considered success.
        genericObjectPool.close();
        for(PooledObject<Channel> pooledObject : channels){
            if( pooledObject != null){
                try {
                    poolableObjectFactory.destroyObject(pooledObject);
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     *
     * @param properties
     * @throws IOException
     * @throws java.util.NoSuchElementException //When a pooled obj cannot obtained
     */
    @Override
    protected void listenImpl(TransportListenProperties properties) throws IOException {
        final boolean noAutoAck = false;
        Channel channel = borrowChannel();
        try {
            AMQPListenProperties listenProperties = (AMQPListenProperties)properties;
            AMQPQueueConsumer consumer = amqpQueueConsumerFactory.build(channel, listenProperties);

            if(listenProperties.isDrainOnListen()){
                channel.queuePurge(listenProperties.getQueue());
            }

            channel.basicConsume(listenProperties.getQueue(), noAutoAck, consumer);
        }
        finally {
            if (channel != null){
                returnChannel(channel);
            }
        }
    }

    protected Channel borrowChannel() {
        Channel channel = null;
        try {
            channel = genericObjectPool.borrowObject();
        } catch (Exception e) {
            throw new NoSuchElementException();
        }

        return channel;
    }

    @Override
    protected void stopImpl() throws IOException {
        genericObjectPool.close();
    }

    /**
     *
     * @param bundle
     * @param properties
     * @return
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws java.util.NoSuchElementException //When a pooled obj cannot obtained
     */
    @Override
    protected boolean publishImpl(TransportMessageBundle bundle, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        Channel channel = borrowChannel();

        try{
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
        finally {
            returnChannel(channel);
        }
    }

    protected void returnChannel(Channel channel) {
        genericObjectPool.returnObject(channel);
    }

    /**
     *
     * @param messageBundles
     * @param properties
     * @param <E>
     * @return
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws java.util.NoSuchElementException //When a pooled obj cannot obtained
     */
    @Override
    protected <E> boolean transactionalPublishImpl(Collection<E> messageBundles, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        Channel channel = borrowChannel();

        try{
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
        finally {
            returnChannel(channel);
        }
    }
}
