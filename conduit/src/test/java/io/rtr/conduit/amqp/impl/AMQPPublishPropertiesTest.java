package io.rtr.conduit.amqp.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AMQPPublishPropertiesTest {

    @Test
    void builderPopulatesAllFields() {
        assertThat(populatedProperties()).hasNoNullFieldsOrProperties();
    }

    @Test
    void builderCopyEqualityCheck() {
        final AMQPPublishProperties properties = populatedProperties();

        assertThat(AMQPPublishProperties.builder().of(properties).build())
                .usingRecursiveComparison()
                .isEqualTo(properties);
    }

    private static AMQPPublishProperties populatedProperties() {
        return AMQPPublishProperties.builder()
                .exchange("an.exchange")
                .routingKey("routing.key")
                .timeout(321)
                .confirmEnabled(true)
                .build();
    }
}
