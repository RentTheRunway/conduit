package io.rtr.conduit.amqp.impl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
