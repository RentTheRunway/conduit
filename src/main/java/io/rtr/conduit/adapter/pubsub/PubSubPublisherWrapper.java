package io.rtr.conduit.adapter.pubsub;

import com.google.api.core.ApiFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.rtr.conduit.adapter.MessageFuture;
import io.rtr.conduit.adapter.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PubSubPublisherWrapper implements Publisher {
//    private static final Logger log = LoggerFactory.getLogger(PubSubPublisherWrapper.class);

    private com.google.cloud.pubsub.v1.Publisher publisherImpl;
    
    PubSubPublisherWrapper( com.google.cloud.pubsub.v1.Publisher publisher){
        publisherImpl = publisher;
    }
    
    @Override
    public MessageFuture publish(Object mesg) {
        final ByteString data = ByteString.copyFromUtf8(mesg.toString());
        final PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        final ApiFuture<String> future = publisherImpl.publish(pubsubMessage);
        return new PubSubMessageFuture(future);
    }

    public Object getPublisherImpl(){
        return publisherImpl;
    }
    
    @Override
    public void close() {
        try {
            publisherImpl.shutdown();
            publisherImpl.awaitTermination(1, TimeUnit.MINUTES);
        } catch (Exception e){
            System.err.println(e);
//            log.error("build error",e);
        }
    }
}
