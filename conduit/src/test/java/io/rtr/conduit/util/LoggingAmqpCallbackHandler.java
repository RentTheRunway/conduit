package io.rtr.conduit.util;

import com.rabbitmq.client.ShutdownSignalException;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.ActionResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class LoggingAmqpCallbackHandler implements AMQPConsumerCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingAmqpCallbackHandler.class);

    @Override
    public ActionResponse handle(AMQPMessageBundle messageBundle) {
        LOGGER.info(
                "Received message: tag={}, messageProperties={}, envelope={}, body={}",
                messageBundle.getConsumerTag(),
                messageBundle.getBasicProperties().toString(),
                messageBundle.getEnvelope().toString(),
                new String(messageBundle.getBody(), StandardCharsets.UTF_8));
        return ActionResponse.acknowledge();
    }

    @Override
    public void notifyOfActionFailure(Exception e) {
        LOGGER.error("Failed to handle message", e);
    }

    @Override
    public void notifyOfShutdown(String consumerTag, ShutdownSignalException sig) {
        LOGGER.warn("Connection shutting down, consumerTag={}", consumerTag, sig);
    }
}
