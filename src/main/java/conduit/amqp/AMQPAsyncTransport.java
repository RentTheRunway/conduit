package conduit.amqp;

import conduit.amqp.consumer.AMQPAsyncQueueConsumer;
import conduit.transport.TransportListenProperties;

import java.io.IOException;

public class AMQPAsyncTransport extends AMQPTransport {
    public AMQPAsyncTransport(String host, int port) {
        super(host, port);
    }

    @Override
    protected void listenImpl(TransportListenProperties properties) throws IOException {
        final boolean noAutoAck = false;

        AMQPAsyncListenProperties listenProperties = (AMQPAsyncListenProperties)properties;
        AMQPAsyncQueueConsumer consumer = new AMQPAsyncQueueConsumer(
                getChannel()
              , listenProperties.getCallback()
              , listenProperties.getThreshold()
        );

        getChannel().basicConsume(listenProperties.getQueue(), noAutoAck, consumer);
    }
}
