package io.rtr.conduit.amqp.impl;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class AMQPSyncConsumerBuilderTest {

    @Test(expected = IllegalArgumentException.class)
    public void testValidationDynamicWithNullRoutingKey(){
        AMQPSyncConsumerBuilder.builder()
                .dynamicQueueCreation(true)
                .dynamicQueueRoutingKey(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationDynamicWithRoutingKeyAndQueue(){
        AMQPSyncConsumerBuilder.builder()
                .dynamicQueueCreation(true)
                .queue("myq")
                .dynamicQueueRoutingKey("myRouter")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationExchangeRequired(){
        AMQPSyncConsumerBuilder.builder()
                .dynamicQueueCreation(true)
                .dynamicQueueRoutingKey("myRouter")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationQueueRequiredWhenNotDynamic(){
        AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .build();
    }

    @Test
    public void testDefaultDynamic(){
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .dynamicQueueCreation(true)
                .dynamicQueueRoutingKey("myRouter");
        AMQPCommonListenProperties commonListenProperties = amqpSyncConsumerBuilder.buildListenProperties();

        assertEquals("When prefetch isn't set, default to ", 1, commonListenProperties.getPrefetchCount());
        assertEquals("When threshold isn't set, default to ", 10, commonListenProperties.getThreshold());
        assertEquals("When poisonPrefix not set, default to ", "", commonListenProperties.getPoisonPrefix());
        assertEquals("When poisonQEnabled not set, default to ", true, commonListenProperties.isPoisonQueueEnabled());
        assertEquals("When isAutoDeleteQueue not set, default to ", false, commonListenProperties.isAutoDeleteQueue());
        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testDefaultExplicit(){
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .queue("queue");
        AMQPCommonListenProperties commonListenProperties = amqpSyncConsumerBuilder
                .buildListenProperties();

        assertEquals("When prefetch isn't set, default to ", 1, commonListenProperties.getPrefetchCount());
        assertEquals("When threshold isn't set, default to ", 10, commonListenProperties.getThreshold());
        assertEquals("When poisonPrefix not set, default to ", "", commonListenProperties.getPoisonPrefix());
        assertEquals("When poisonQEnabled not set, default to ", true, commonListenProperties.isPoisonQueueEnabled());
        assertEquals("When isAutoDeleteQueue not set, default to ", false, commonListenProperties.isAutoDeleteQueue());
        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testAutoCreateAndBindDefault(){
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .autoCreateAndBind(
                "exchange",
                AMQPConsumerBuilder.ExchangeType.CONSISTENT_HASH,
                "queue",
                "routingKey"
            );
        AMQPCommonListenProperties commonListenProperties = amqpSyncConsumerBuilder
            .buildListenProperties();

        assertEquals("autoCreateAndBind() creates a durable (non-auto-delete) queue by default",
            false, commonListenProperties.isAutoDeleteQueue());
        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testAutoCreateAndBindWithAutoDeleteQueue(){
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .autoCreateAndBind(
                "exchange",
                AMQPConsumerBuilder.ExchangeType.CONSISTENT_HASH,
                "queue",
                "routingKey",
                true
            );
        AMQPCommonListenProperties commonListenProperties = amqpSyncConsumerBuilder
            .buildListenProperties();

        assertEquals("When isAutoDeleteQueue is set to true, autoCreateAndBind() creates an auto-delete queue",
            true, commonListenProperties.isAutoDeleteQueue());
        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testSettingCredsAndSharedConnectionThrows(){
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .queue("queue")
                .username("bob")
                .sharedConnection(mock(AMQPConnection.class));

        Assert.assertThrows(IllegalArgumentException.class, amqpSyncConsumerBuilder::build);
    }

    @Test
    public void testSettingVhostAndSharedConnectionThrows(){
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .queue("queue")
                .virtualHost("something")
                .sharedConnection(mock(AMQPConnection.class));

        Assert.assertThrows(IllegalArgumentException.class, amqpSyncConsumerBuilder::build);
    }

    @Test
    public void testSettingOnlySharedConnectionDoesNotThrow(){
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .queue("queue")
                .sharedConnection(mock(AMQPConnection.class));

        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testSettingOnlyCredsAndVhostDoesNotThrow(){
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .queue("queue")
                .username("bob")
                .password("bob")
                .virtualHost("bob");

        amqpSyncConsumerBuilder.build();
    }
}
