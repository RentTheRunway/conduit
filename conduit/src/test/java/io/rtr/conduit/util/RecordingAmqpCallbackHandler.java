package io.rtr.conduit.util;

import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.ActionResponse;

import java.util.ArrayList;
import java.util.List;

public class RecordingAmqpCallbackHandler extends LoggingAmqpCallbackHandler {
    private final List<AMQPMessageBundle> capturedMessages = new ArrayList<>();

    @Override
    public ActionResponse handle(AMQPMessageBundle messageBundle) {
        capturedMessages.add(messageBundle);
        return ActionResponse.acknowledge();
    }

    public List<AMQPMessageBundle> getCapturedMessages() {
        return capturedMessages;
    }
}
