package io.rtr.conduit.adapter.ampq;

import com.google.api.core.ApiFuture;
import io.rtr.conduit.adapter.MessageFuture;
import io.rtr.conduit.adapter.Publisher;
import io.rtr.conduit.adapter.pubsub.PubSubMessageFuture;
import io.rtr.conduit.adapter.pubsub.PubSubPublisherWrapper;
import io.rtr.conduit.amqp.AMQPMessageBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class AmpqPublisherWrapper implements Publisher {
//    private static final Logger log = LoggerFactory.getLogger(AmpqPublisherWrapper.class);
    private io.rtr.conduit.amqp.publisher.Publisher publisherImpl;


    AmpqPublisherWrapper(io.rtr.conduit.amqp.publisher.Publisher publisher){
        publisherImpl = publisher;
    }
    
    @Override
    public MessageFuture publish(Object mesg) {
        try {
            publisherImpl.publish(new AMQPMessageBundle(mesg.toString()));
        } catch ( Exception e ){
            System.err.println(e);
//            log.error("publish",e);
        }
        return null;
    }

    public Object getPublisherImpl(){
        return publisherImpl;
    }
    
    @Override
    public void close() {
        try {
            publisherImpl.close();
        } catch (Exception e){
            System.err.println(e);
//            log.error("build error",e);
        }
    }
}
