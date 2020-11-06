package io.rtr.conduit.amqp.impl;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

public class AMQPConnectionPropertiesTest {
    @Test
    public void testValidateBuilder() {
        AMQPConnectionProperties properties = AMQPConnectionProperties.builder()
                .username("Anna")
                .password("Anna's password")
                .automaticRecoveryEnabled(true)
                .connectionTimeout(Duration.ofHours(8))
                .heartbeatInterval(Duration.ofSeconds(2))
                .virtualHost("Anna's vhost")
                .build();

        Assert.assertEquals("Anna", properties.getUsername());
        Assert.assertEquals("Anna's password", properties.getPassword());
        Assert.assertEquals("Anna's vhost", properties.getVirtualHost());
        Assert.assertEquals(Duration.ofHours(8).toMillis(), properties.getConnectionTimeout());
        Assert.assertEquals(2, properties.getHeartbeatInterval());
    }
}
