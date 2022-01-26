package io.rtr.conduit.amqp;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AMQPMessageBundleTest {

    @Test
    public void testMessageBundleHeaders() {
        // create message with default headers
        AMQPMessageBundle bundle1 = new AMQPMessageBundle("test");
        Map<String, Object> headers1 = new HashMap<>(bundle1.getBasicProperties().getHeaders());

        // our additional headers
        Map<String,Object> headers2 = new HashMap<>();
        headers2.put("foo", 1);
        headers2.put("bar", "baz");

        // add our additional headers to default headers
        headers1.putAll(headers2);

        // ensure that headers include our headers and default headers
        AMQPMessageBundle bundle2 = new AMQPMessageBundle("test", headers2);
        headers2 = new HashMap<>(bundle2.getBasicProperties().getHeaders());

        assertTrue(headers2.size() > 0);
        assertEquals(headers1, headers2);
    }
}
