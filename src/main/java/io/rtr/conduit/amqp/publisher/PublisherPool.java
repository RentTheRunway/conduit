package io.rtr.conduit.amqp.publisher;

import io.rtr.conduit.amqp.impl.AMQPPublisherBuilder;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * A pool of Conduit publishers, which when configured with
 * default pool settings test connectivity upon borrowing
 * from the pool
 * <p/>
 * Usage
 * Publisher publisher = null;
 * try{
 * publisher = pool.borrowObject();
 * //do some publishing ...
 * }
 * finally{
 * pool.returnObject(publisher)
 * }
 */
public class PublisherPool extends GenericObjectPool<Publisher> {
    private static class PooledPublisherFactory extends BasePooledObjectFactory<Publisher> {
        private AMQPPublisherBuilder builder;

        public PooledPublisherFactory(AMQPPublisherBuilder builder) {
            this.builder = builder;
        }

        @Override
        public Publisher create() throws Exception {
            Publisher publisher = builder.build();
            publisher.connect();
            return publisher;
        }

        @Override
        public PooledObject<Publisher> wrap(Publisher obj) {
            return new DefaultPooledObject<Publisher>(obj);
        }

        @Override
        public boolean validateObject(PooledObject<Publisher> p) {
            return p.getObject().isOpen();
        }

        public void destroyObject(PooledObject<Publisher> p) throws Exception {
            p.getObject().close();
        }
    }

    public PublisherPool(AMQPPublisherBuilder builder, GenericObjectPoolConfig config) {
        super(new PooledPublisherFactory(builder), config);
    }

    public static GenericObjectPoolConfig getDefaultPoolConfig() {
        GenericObjectPoolConfig defaultPoolConfig = new GenericObjectPoolConfig();
        defaultPoolConfig.setMaxIdle(Integer.MAX_VALUE);
        defaultPoolConfig.setMaxTotal(8);
        defaultPoolConfig.setMaxWaitMillis(500);
        defaultPoolConfig.setBlockWhenExhausted(true);
        defaultPoolConfig.setTestOnBorrow(true);
        defaultPoolConfig.setTestOnCreate(false);
        defaultPoolConfig.setTestOnReturn(false);
        return defaultPoolConfig;
    }
}
