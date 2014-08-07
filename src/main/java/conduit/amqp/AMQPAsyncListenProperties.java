package conduit.amqp;


import conduit.transport.TransportListenProperties;

public class AMQPAsyncListenProperties implements TransportListenProperties {
    private AMQPAsyncConsumerCallback callback;
    private String exchange;
    private String queue;
    private int threshold;
    private int prefetchCount;
    private boolean poisonQueueEnabled;

    public AMQPAsyncListenProperties(
            AMQPAsyncConsumerCallback callback
          , String exchange
          , String queue
    ) {
        this(callback, exchange, queue, 10);
    }

    public AMQPAsyncListenProperties(
            AMQPAsyncConsumerCallback callback
          , String exchange
          , String queue
          , int threshold
    ) {
        this(callback, exchange, queue, threshold, 0);
    }
    
    public AMQPAsyncListenProperties(
            AMQPAsyncConsumerCallback callback
          , String exchange
          , String queue
          , int threshold
          , int prefetchCount
    ) {
        this(callback, exchange, queue, threshold, prefetchCount, true);
    }

    public AMQPAsyncListenProperties(
            AMQPAsyncConsumerCallback callback
          , String exchange
          , String queue
          , int threshold
          , int prefetchCount
          , boolean poisonQueueEnabled
    ) {
        this.callback = callback;
        this.exchange = exchange;
        this.queue = queue;
        this.threshold = threshold;
        this.prefetchCount = prefetchCount;
        this.poisonQueueEnabled = poisonQueueEnabled;
    }

    public AMQPAsyncConsumerCallback getCallback() {
        return callback;
    }

    public String getExchange() {
        return exchange;
    }

    public String getQueue() {
        return queue;
    }

    public int getThreshold() {
        return threshold;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }
    
    public boolean isPoisonQueueEnabled() {
    	return poisonQueueEnabled;
    }
}
