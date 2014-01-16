package conduit.amqp.consumer;

import com.rabbitmq.client.Channel;
import conduit.amqp.AMQPListenProperties;

public class AMQPQueueConsumerFactory {

    public AMQPQueueConsumer build(Channel channel, AMQPListenProperties properties){
        return new AMQPQueueConsumer(
                channel,
                properties.getCallback(),
                properties.getThreshold()
        );
    }
}
