package io.rtr.conduit.amqp;

import com.rabbitmq.client.AMQP;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AMQPMessageBundleTest {

    @Test
    void testMessageBundleHeaders() {
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

        assertFalse(headers2.isEmpty());
        assertEquals(headers1, headers2);
    }

    @Test
    void buildMessageWithHeaders_populatesPropertiesAndBody() {
        final AMQPMessageBundle messageBundle = AMQPMessageBundle.builder()
                .header("foo", 1)
                .header("foo", null)
                .header("bar", "baz")
                .header("foo2", 2)
                .body("A message")
                .build();

        assertThat(messageBundle.getBasicProperties())
                .isNotNull()
                .extracting(AMQP.BasicProperties::getHeaders)
                .satisfies(headers -> assertThat(headers)
                        .containsEntry("bar", "baz")
                        .containsEntry("foo2", 2)
                        .doesNotContainKey("foo"));
        assertThat(messageBundle.getBody())
                .satisfies(bytes -> assertThat(new String(bytes))
                        .isEqualTo("A message"));
    }

    @Test
    void settingBothBasicPropertiesAndHeaders_throws() {
        final AMQPMessageBundle.Builder builder = AMQPMessageBundle.builder()
                .basicProperties(new AMQP.BasicProperties())
                .headers(singletonMap("foo", 1));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Both basicProperties and headers are set");
    }
}
