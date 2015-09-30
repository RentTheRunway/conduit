package io.rtr.conduit.amqp.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AMQPAsyncConsumerBuilderTest {

    @Test(expected = IllegalArgumentException.class)
    public void testValidationDynamicWithNullRoutingKey(){
        AMQPAsyncConsumerBuilder.builder()
                .dynamicQueueCreation(true)
                .dynamicQueueRoutingKey(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationDynamicWithRoutingKeyAndQueue(){
        AMQPAsyncConsumerBuilder.builder()
                .dynamicQueueCreation(true)
                .queue("myq")
                .dynamicQueueRoutingKey("myRouter")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationExchangeRequired(){
        AMQPAsyncConsumerBuilder.builder()
                .dynamicQueueCreation(true)
                .dynamicQueueRoutingKey("myRouter")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationQueueRequiredWhenNotDynamic(){
        AMQPAsyncConsumerBuilder.builder()
                .exchange("exchange")
                .build();
    }

    @Test
    public void testValidationBasicConfig(){
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
                .ensureBasicConfig("exchange", AMQPConsumerBuilder.ExchangeType.DIRECT, "queue", "routingKey");
        AMQPCommonListenProperties commonListenProperties = amqpAsyncConsumerBuilder.buildListenProperties();

        assertEquals("Can not ensureBasicConfig and be dynamic", false, commonListenProperties.isDynamicQueueCreation());
        assertEquals("Queue should be: ", "queue", commonListenProperties.getQueue());
        assertEquals("Exchange should be: ", "exchange", commonListenProperties.getExchange());
        assertEquals("RoutingKey should be: ", "routingKey", commonListenProperties.getRoutingKey());
        assertEquals("ExchangeType should be: ", AMQPConsumerBuilder.ExchangeType.DIRECT.toString(), commonListenProperties.getExchangeType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationBasicConfigWithNullRoutingKey(){
        AMQPAsyncConsumerBuilder.builder()
                .ensureBasicConfig("exchange", AMQPConsumerBuilder.ExchangeType.DIRECT, "queue", null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationBasicConfigWithNullQueue(){
        AMQPAsyncConsumerBuilder.builder()
                .ensureBasicConfig("exchange", AMQPConsumerBuilder.ExchangeType.DIRECT, null, "routingKey")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationBasicConfigWithDynamic(){
        AMQPAsyncConsumerBuilder.builder()
                .dynamicQueueCreation(true)
                .ensureBasicConfig("exchange", AMQPConsumerBuilder.ExchangeType.DIRECT, null, "routingKey")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationBasicConfigWithPoisonFanout(){
        AMQPAsyncConsumerBuilder.builder()
                .poisonQueueEnabled(true)
                .ensureBasicConfig("exchange", AMQPConsumerBuilder.ExchangeType.FANOUT, null, "routingKey")
                .build();
    }

    @Test
    public void testDefaultDynamic(){
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
                .exchange("exchange")
                .dynamicQueueCreation(true)
                .dynamicQueueRoutingKey("myRouter");
        AMQPCommonListenProperties commonListenProperties = amqpAsyncConsumerBuilder.buildListenProperties();

        assertEquals("When prefetch isn't set, default to ", 100, commonListenProperties.getPrefetchCount());
        assertEquals("When threshold isn't set, default to ", 10, commonListenProperties.getThreshold());
        assertEquals("When poisonPrefix not set, default to ", "", commonListenProperties.getPoisonPrefix());
        assertEquals("When poisonQEnabled not set, default to ", true, commonListenProperties.isPoisonQueueEnabled());
        amqpAsyncConsumerBuilder.build();
    }

    @Test
    public void testDefaultExplicit(){
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
                .exchange("exchange")
                .queue("queue");
        AMQPCommonListenProperties commonListenProperties = amqpAsyncConsumerBuilder
                .buildListenProperties();

        assertEquals("When prefetch isn't set, default to ", 100, commonListenProperties.getPrefetchCount());
        assertEquals("When threshold isn't set, default to ", 10, commonListenProperties.getThreshold());
        assertEquals("When poisonPrefix not set, default to ", "", commonListenProperties.getPoisonPrefix());
        assertEquals("When poisonQEnabled not set, default to ", true, commonListenProperties.isPoisonQueueEnabled());
        amqpAsyncConsumerBuilder.build();
    }
}
