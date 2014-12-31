package io.rtr.conduit.amqp.publisher;

import io.rtr.conduit.amqp.impl.AMQPPublisherBuilder;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PublisherPoolTest {
    @Test
    public void testBasicBorrowLogic() {
        AMQPPublisherBuilder amqpPublisherBuilder = mock(AMQPPublisherBuilder.class);
        Publisher publisher = mock(Publisher.class);
        when(publisher.isOpen()).thenReturn(true);
        when(amqpPublisherBuilder.build()).thenReturn(publisher);

        GenericObjectPoolConfig defaultPoolConfig = PublisherPool.getDefaultPoolConfig();
        defaultPoolConfig.setMaxTotal(2);
        PublisherPool publisherPool = new PublisherPool(amqpPublisherBuilder, defaultPoolConfig);
        try {
            assertEquals("Should be 0 idle", 0L, publisherPool.getNumIdle());
            Publisher borrowObject = publisherPool.borrowObject();
            assertEquals("Should be 1 borrowed", 1L, publisherPool.getBorrowedCount());
            assertEquals("Should be 0 idle", 0L, publisherPool.getNumIdle());

            publisherPool.returnObject(borrowObject);
            assertEquals("Should be 1 idle", 1L, publisherPool.getNumIdle());
        } catch (Exception e) {
            fail("Exception shouldn't be thrown");
        }
    }

    @Test
    public void testExhaustion() {
        AMQPPublisherBuilder amqpPublisherBuilder = mock(AMQPPublisherBuilder.class);
        Publisher publisher = mock(Publisher.class);
        when(publisher.isOpen()).thenReturn(true);
        when(amqpPublisherBuilder.build()).thenReturn(publisher);

        GenericObjectPoolConfig defaultPoolConfig = PublisherPool.getDefaultPoolConfig();
        defaultPoolConfig.setMaxTotal(2);
        PublisherPool publisherPool = new PublisherPool(amqpPublisherBuilder, defaultPoolConfig);
        try {
            publisherPool.borrowObject();
            publisherPool.borrowObject();
            publisherPool.borrowObject();
        } catch (Exception e) {
            assertEquals("Should timeout", "Timeout waiting for idle object", e.getMessage());
        }
    }

    @Test
    public void testEvictionOfClosed() {
        AMQPPublisherBuilder amqpPublisherBuilder = mock(AMQPPublisherBuilder.class);
        Publisher publisher1 = mock(Publisher.class);
        Publisher publisher2 = mock(Publisher.class);
        Publisher publisher3 = mock(Publisher.class);
        when(publisher1.isOpen()).thenReturn(true);
        when(publisher2.isOpen())
                .thenReturn(true)
                .thenReturn(false);
        when(publisher3.isOpen()).thenReturn(true);

        when(amqpPublisherBuilder.build())
                .thenReturn(publisher1)
                .thenReturn(publisher2)
                .thenReturn(publisher3);

        GenericObjectPoolConfig defaultPoolConfig = PublisherPool.getDefaultPoolConfig();
        defaultPoolConfig.setMaxTotal(2);
        PublisherPool publisherPool = new PublisherPool(amqpPublisherBuilder, defaultPoolConfig);
        try {
            //First is open and fine
            publisherPool.borrowObject();
            //Second is open and fine
            Publisher borrowObject2 = publisherPool.borrowObject();
            assertEquals("Should be 0 idle", 0L, publisherPool.getNumIdle());
            //Returning this one, next call to open will say false
            publisherPool.returnObject(borrowObject2);
            assertEquals("Should be 1 idle", 1L, publisherPool.getNumIdle());
            Publisher borrowObject3 = publisherPool.borrowObject();
            assertEquals("Should have to create a new publisher, as the second was closed", publisher3, borrowObject3);
            assertEquals("Should have to create 1 more than max", 3L, publisherPool.getCreatedCount());
            assertEquals("Should be 2 active", 2L, publisherPool.getNumActive());
            assertEquals("Should be 0 idle", 0L, publisherPool.getNumIdle());
        } catch (Exception e) {
            fail("Exception shouldn't be thrown");
        }
    }
}
