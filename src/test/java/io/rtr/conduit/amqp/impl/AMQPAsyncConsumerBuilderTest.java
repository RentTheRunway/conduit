package io.rtr.conduit.amqp.impl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.rtr.conduit.amqp.impl.AMQPConsumerBuilder.ExchangeType.CONSISTENT_HASH;
import static io.rtr.conduit.amqp.impl.AMQPConsumerBuilder.ExchangeType.DIRECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class AMQPAsyncConsumerBuilderTest {

    @Test
    public void testValidationDynamicWithNullRoutingKey() {
        assertThrows(IllegalArgumentException.class, () -> AMQPAsyncConsumerBuilder.builder()
            .dynamicQueueCreation(true)
            .dynamicQueueRoutingKey(null)
            .build());
    }

    @Test
    public void testValidationDynamicWithRoutingKeyAndQueue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPAsyncConsumerBuilder.builder()
            .dynamicQueueCreation(true)
            .queue("myq")
            .dynamicQueueRoutingKey("myRouter")
            .build());
    }

    @Test
    public void testValidationExchangeRequired() {
        assertThrows(IllegalArgumentException.class, () -> AMQPAsyncConsumerBuilder.builder()
            .dynamicQueueCreation(true)
            .dynamicQueueRoutingKey("myRouter")
            .build());
    }

    @Test
    public void testValidationQueueRequiredWhenNotDynamic() {
        assertThrows(IllegalArgumentException.class, () -> AMQPAsyncConsumerBuilder.builder()
            .exchange("exchange")
            .build());
    }

    @Test
    public void testValidationAutoCreateAndBind() {
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
            .autoCreateAndBind("exchange", DIRECT, "queue", "routingKey");
        AMQPCommonListenProperties commonListenProperties = amqpAsyncConsumerBuilder.buildListenProperties();

        // check that the properties got set correctly
        assertEquals("queue", commonListenProperties.getQueue(), "Queue should be: ");
        assertFalse(commonListenProperties.isAutoDeleteQueue(), "isAutoDeleteQueue should be: ");
        assertEquals("exchange", commonListenProperties.getExchange(), "Exchange should be: ");
        assertEquals("routingKey", commonListenProperties.getRoutingKey(), "RoutingKey should be: ");
        assertEquals(DIRECT.toString(), commonListenProperties.getExchangeType(), "ExchangeType should be: ");

        // Now check that the defaults validate.
        amqpAsyncConsumerBuilder.validate();
    }

    @Test
    public void testAutoCreateAndBindWithAutoDeleteQueue() {
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
            .autoCreateAndBind("exchange", CONSISTENT_HASH, "queue", true, "routingKey");
        AMQPCommonListenProperties commonListenProperties = amqpAsyncConsumerBuilder.buildListenProperties();

        // check that the properties got set correctly
        assertEquals("queue", commonListenProperties.getQueue(), "Queue should be: ");
        assertTrue(commonListenProperties.isAutoDeleteQueue(), "isAutoDeleteQueue should be: ");
        assertEquals("exchange", commonListenProperties.getExchange(), "Exchange should be: ");
        assertEquals("routingKey", commonListenProperties.getRoutingKey(), "RoutingKey should be: ");
        assertEquals(CONSISTENT_HASH.toString(), commonListenProperties.getExchangeType(), "ExchangeType should be: ");

        // Now check that the defaults validate.
        amqpAsyncConsumerBuilder.validate();
    }

    @Test
    public void testValidationAutoCreateAndBindWithNullRoutingKey() {
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
            .autoCreateAndBind("exchange", DIRECT, "queue", null);

        AMQPCommonListenProperties commonListenProperties = amqpAsyncConsumerBuilder.buildListenProperties();
        Assertions.assertNotNull(commonListenProperties.getRoutingKey());
        assertEquals("", commonListenProperties.getRoutingKey());
    }

    @Test
    public void testValidationAutoCreateAndBindWithNullQueue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPAsyncConsumerBuilder.builder()
            .autoCreateAndBind("exchange", DIRECT, null, "routingKey")
            .build());
    }

    @Test
    public void testValidationAutoCreateAndBindWithDynamic() {
        assertThrows(IllegalArgumentException.class, () -> AMQPAsyncConsumerBuilder.builder()
            .dynamicQueueCreation(true)
            .autoCreateAndBind("exchange", DIRECT, "queue", "routingKey")
            .build());
    }

    @Test
    public void testValidationAutoCreateAndBindWithPoisonFanout() {
        assertThrows(IllegalArgumentException.class, () -> AMQPAsyncConsumerBuilder.builder()
            .poisonQueueEnabled(true)
            .autoCreateAndBind("exchange", AMQPConsumerBuilder.ExchangeType.FANOUT, "queue", "routingKey")
            .build());
    }

    @Test
    public void testDefaultDynamic() {
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
            .exchange("exchange")
            .dynamicQueueCreation(true)
            .dynamicQueueRoutingKey("myRouter");
        AMQPCommonListenProperties commonListenProperties = amqpAsyncConsumerBuilder.buildListenProperties();

        assertEquals(100, commonListenProperties.getPrefetchCount(), "When prefetch isn't set, default to ");
        assertEquals(10, commonListenProperties.getThreshold(), "When threshold isn't set, default to ");
        assertEquals("", commonListenProperties.getPoisonPrefix(), "When poisonPrefix not set, default to ");
        assertTrue(commonListenProperties.isPoisonQueueEnabled(), "When poisonQEnabled not set, default to ");
        amqpAsyncConsumerBuilder.build();
    }

    @Test
    public void testDefaultExplicit() {
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
            .exchange("exchange")
            .queue("queue");
        AMQPCommonListenProperties commonListenProperties = amqpAsyncConsumerBuilder.buildListenProperties();

        assertEquals(100, commonListenProperties.getPrefetchCount(), "When prefetch isn't set, default to ");
        assertEquals(10, commonListenProperties.getThreshold(), "When threshold isn't set, default to ");
        assertEquals("", commonListenProperties.getPoisonPrefix(), "When poisonPrefix not set, default to ");
        assertTrue(commonListenProperties.isPoisonQueueEnabled(), "When poisonQEnabled not set, default to ");
        amqpAsyncConsumerBuilder.build();
    }

    @Test
    public void testSettingCredsAndSharedConnectionThrows() {
        AMQPAsyncConsumerBuilder amqpAsyncConsumerBuilder = AMQPAsyncConsumerBuilder.builder()
            .exchange("exchange")
            .queue("queue")
            .username("bob")
            .sharedConnection(mock(AMQPConnection.class));

        assertThrows(IllegalArgumentException.class, amqpAsyncConsumerBuilder::build);
    }
}
