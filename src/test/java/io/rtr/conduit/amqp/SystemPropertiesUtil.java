package io.rtr.conduit.amqp;

public class SystemPropertiesUtil {
    public static void withSystemProperty(String name, String value, Runnable body) {
        final String oldValue = System.getProperty(name);
        System.setProperty(name, value);

        try {
            body.run();
        } finally {
            if (oldValue == null) {
                System.clearProperty(name);
            } else {
                System.setProperty(name, oldValue);
            }
        }
    }
}
