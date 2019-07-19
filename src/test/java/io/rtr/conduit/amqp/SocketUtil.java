package io.rtr.conduit.amqp;

import java.io.IOException;
import java.net.ServerSocket;

public final class SocketUtil {
    private SocketUtil() {}

    public static int getAvailablePort() {
        try {
            return new ServerSocket(0).getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
