package conduit.amqp;

import conduit.transport.TransportPublishProperties;

/**
 * User: kmandrika
 * Date: 1/8/13
 */
public class AMQPPublishProperties implements TransportPublishProperties {
    private String exchange;
    private String routingKey;
    private long timeout;

    public AMQPPublishProperties(String exchange, String routingKey, long timeout) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.timeout = timeout;
    }

    public AMQPPublishProperties(String exchange, String routingKey) {
        this(exchange, routingKey, 100);
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public long getTimeout() {
        return timeout;
    }
}
