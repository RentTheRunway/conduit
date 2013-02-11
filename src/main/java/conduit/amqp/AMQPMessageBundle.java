package conduit.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import conduit.transport.TransportMessageBundle;

import java.util.HashMap;
import java.util.Map;

/**
 * User: kmandrika
 * Date: 1/9/13
 */
public class AMQPMessageBundle implements TransportMessageBundle {
    private String consumerTag;
    private Envelope envelope;
    private AMQP.BasicProperties basicProperties;
    private byte[] body;

    private static AMQP.BasicProperties initialProperties() {
        Map<String, Object> headers = new HashMap<String, Object>();

        headers.put("conduit-retry-count", 0);

        return new AMQP.BasicProperties()
                .builder()
                .deliveryMode(2 /*persistent*/)
                .priority(0)
                .headers(headers)
                .contentType("text/plain")
                .build();
    }

    public AMQPMessageBundle(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] body) {
        this.consumerTag = consumerTag;
        this.envelope = envelope;
        this.basicProperties = basicProperties;
        this.body = body;
    }

    public AMQPMessageBundle(String message) {
        this(null, null, initialProperties(), message.getBytes());
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public AMQP.BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public byte[] getBody() {
        return body;
    }
}
