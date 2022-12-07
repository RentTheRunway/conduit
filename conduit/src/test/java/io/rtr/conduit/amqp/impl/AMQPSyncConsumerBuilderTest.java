package io.rtr.conduit.amqp.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class AMQPSyncConsumerBuilderTest {

    @Test
    public void testValidationDynamicWithNullRoutingKey() {
        assertThrows(IllegalArgumentException.class, () -> AMQPSyncConsumerBuilder.builder()
            .dynamicQueueCreation(true)
            .dynamicQueueRoutingKey(null)
            .build());
    }

    @Test
    public void testValidationDynamicWithRoutingKeyAndQueue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPSyncConsumerBuilder.builder()
            .dynamicQueueCreation(true)
            .queue("myq")
            .dynamicQueueRoutingKey("myRouter")
            .build());
    }

    @Test
    public void testValidationExchangeRequired() {
        assertThrows(IllegalArgumentException.class, () -> AMQPSyncConsumerBuilder.builder()
            .dynamicQueueCreation(true)
            .dynamicQueueRoutingKey("myRouter")
            .build());
    }

    @Test
    public void testValidationQueueRequiredWhenNotDynamic() {
        assertThrows(IllegalArgumentException.class, () -> AMQPSyncConsumerBuilder.builder()
            .exchange("exchange")
            .build());
    }

    @Test
    public void testDefaultDynamic() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .exchange("exchange")
            .dynamicQueueCreation(true)
            .dynamicQueueRoutingKey("myRouter");
        AMQPCommonListenProperties commonListenProperties = amqpSyncConsumerBuilder.buildListenProperties();

        assertEquals(1, commonListenProperties.getPrefetchCount(), "When prefetch isn't set, default to ");
        assertEquals(10, commonListenProperties.getThreshold(), "When threshold isn't set, default to ");
        assertEquals("", commonListenProperties.getPoisonPrefix(), "When poisonPrefix not set, default to ");
        assertTrue(commonListenProperties.isPoisonQueueEnabled(), "When poisonQEnabled not set, default to ");
        assertFalse(commonListenProperties.isAutoDeleteQueue(), "When isAutoDeleteQueue not set, default to ");
        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testDefaultExplicit() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .exchange("exchange")
            .queue("queue");
        AMQPCommonListenProperties commonListenProperties = amqpSyncConsumerBuilder.buildListenProperties();

        assertEquals(1, commonListenProperties.getPrefetchCount(), "When prefetch isn't set, default to ");
        assertEquals(10, commonListenProperties.getThreshold(), "When threshold isn't set, default to ");
        assertEquals("", commonListenProperties.getPoisonPrefix(), "When poisonPrefix not set, default to ");
        assertTrue(commonListenProperties.isPoisonQueueEnabled(), "When poisonQEnabled not set, default to ");
        assertFalse(commonListenProperties.isAutoDeleteQueue(), "When isAutoDeleteQueue not set, default to ");
        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testAutoCreateAndBindDefault() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .autoCreateAndBind("exchange", AMQPConsumerBuilder.ExchangeType.CONSISTENT_HASH, "queue", "routingKey");
        AMQPCommonListenProperties commonListenProperties = amqpSyncConsumerBuilder.buildListenProperties();

        assertFalse(commonListenProperties.isAutoDeleteQueue(), "autoCreateAndBind() creates a durable (non-auto-delete) queue by default");
        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testAutoCreateAndBindWithAutoDeleteQueue() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .autoCreateAndBind("exchange", AMQPConsumerBuilder.ExchangeType.CONSISTENT_HASH, "queue", true, "routingKey");
        AMQPCommonListenProperties commonListenProperties = amqpSyncConsumerBuilder.buildListenProperties();

        assertTrue(commonListenProperties.isAutoDeleteQueue(), "When isAutoDeleteQueue is set to true, autoCreateAndBind() creates an auto-delete queue");
        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testSettingCredsAndSharedConnectionThrows() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .exchange("exchange")
            .queue("queue")
            .username("bob")
            .sharedConnection(mock(AMQPConnection.class));

        assertThrows(IllegalArgumentException.class, amqpSyncConsumerBuilder::build);
    }

    @Test
    public void testSettingVhostAndSharedConnectionThrows() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .exchange("exchange")
            .queue("queue")
            .virtualHost("something")
            .sharedConnection(mock(AMQPConnection.class));

        assertThrows(IllegalArgumentException.class, amqpSyncConsumerBuilder::build);
    }

    @Test
    public void testSettingOnlySharedConnectionDoesNotThrow() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .exchange("exchange")
            .queue("queue")
            .sharedConnection(mock(AMQPConnection.class));

        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testSettingOnlyCredsAndVhostDoesNotThrow() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
            .exchange("exchange")
            .queue("queue")
            .username("bob")
            .password("bob")
            .virtualHost("bob");

        amqpSyncConsumerBuilder.build();
    }

    @Test
    public void testDefaultConnectionProperties() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .queue("queue")
                .username("bob")
                .password("bob");

        AMQPConnectionProperties connectionProperties = amqpSyncConsumerBuilder.buildConnectionProperties();
        assertEquals(5000, connectionProperties.getNetworkRecoveryInterval(), "When Network Recovery Interval not set, default to ");
    }

    @Test
    public void testOverrideConnectionProperties() {
        AMQPSyncConsumerBuilder amqpSyncConsumerBuilder = AMQPSyncConsumerBuilder.builder()
                .exchange("exchange")
                .queue("queue")
                .username("bob")
                .password("bob")
                .networkRecoveryInterval(10000L);

        AMQPConnectionProperties connectionProperties = amqpSyncConsumerBuilder.buildConnectionProperties();
        assertEquals(10000, connectionProperties.getNetworkRecoveryInterval(), "When Network Recovery Interval not set, default to ");
    }
}
