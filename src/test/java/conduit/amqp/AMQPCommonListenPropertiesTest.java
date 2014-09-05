package conduit.amqp;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AMQPCommonListenPropertiesTest {

    @Test(expected = IllegalArgumentException.class)
    public void testValidationDynamicWithNullRoutingKey(){
        new AMQPCommonListenProperties.AMQPCommonListenPropertiesBuilder()
                .setDynamicQueueCreation(true)
                .setDynamicQueueRoutingKey(null).createAMQPCommonListenProperties();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationDynamicWithRoutingKeyAndQueue(){
        new AMQPCommonListenProperties.AMQPCommonListenPropertiesBuilder()
                .setDynamicQueueCreation(true)
                .setQueue("myq")
                .setDynamicQueueRoutingKey("myRouter").createAMQPCommonListenProperties();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationExchangeRequired(){
        new AMQPCommonListenProperties.AMQPCommonListenPropertiesBuilder()
                .setDynamicQueueCreation(true)
                .setDynamicQueueRoutingKey("myRouter").createAMQPCommonListenProperties();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationQueueRequiredWhenNotDynamic(){
        new AMQPCommonListenProperties.AMQPCommonListenPropertiesBuilder()
                .setExchange("exchange").createAMQPCommonListenProperties();
    }

    @Test
    public void testDefaultDynamic(){
        AMQPCommonListenProperties commonListenProperties = new AMQPCommonListenProperties.AMQPCommonListenPropertiesBuilder()
                .setExchange("exchange")
                .setDynamicQueueCreation(true)
                .setDynamicQueueRoutingKey("myRouter").createAMQPCommonListenProperties();
        assertEquals("When prefetch isn't set, default to ", 0, commonListenProperties.getPrefetchCount());
        assertEquals("When threshold isn't set, default to ", 10, commonListenProperties.getThreshold());
        assertEquals("When poisonPrefix not set, default to ", "", commonListenProperties.getPoisonPrefix());
    }

    @Test
    public void testDefaultExplicit(){
        AMQPCommonListenProperties commonListenProperties = new AMQPCommonListenProperties.AMQPCommonListenPropertiesBuilder()
                .setExchange("exchange")
                .setQueue("queue")
                .createAMQPCommonListenProperties();
        assertEquals("When prefetch isn't set, default to ", 0, commonListenProperties.getPrefetchCount());
        assertEquals("When threshold isn't set, default to ", 10, commonListenProperties.getThreshold());
        assertEquals("When poisonPrefix not set, default to ", "", commonListenProperties.getPoisonPrefix());
    }
}
