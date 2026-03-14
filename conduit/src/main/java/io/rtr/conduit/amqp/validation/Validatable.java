package io.rtr.conduit.amqp.validation;

public abstract class Validatable {

    protected abstract void validate();

    protected void assertNotNull(final Object value, final String argument) {
        if (value == null) {
            throw new IllegalArgumentException(
                    String.format("Argument %s cannot be null", argument));
        }
    }
}
