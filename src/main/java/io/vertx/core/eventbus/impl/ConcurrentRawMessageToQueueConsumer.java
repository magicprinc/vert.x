package io.vertx.core.eventbus.impl;

import io.vertx.core.eventbus.Message;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.function.BiPredicate;

/**
 * This class is optimized for performance when used on the same event loop it was created on.
 * However it can be used safely from other threads.
 *
 * The internal state is protected using the synchronized keyword. If always used on the same event loop, then
 * we benefit from biased locking which makes the overhead of synchronized near zero.

 Important! {@link Message} is not processed by {@link EventBusImpl#inboundInterceptors} i.e. is raw/direct-from-sender Message!

 @see MessageConsumerImpl
 @see ReplyHandler
 @see MessageToQueueConsumer
 */
public class ConcurrentRawMessageToQueueConsumer<T> extends RawMessageToQueueConsumer<T> {

  private static final Logger log = LoggerFactory.getLogger(ConcurrentRawMessageToQueueConsumer.class);

  public ConcurrentRawMessageToQueueConsumer (ContextInternal context, EventBusImpl eventBus, String address, boolean localOnly, BiPredicate<Message<T>,RawMessageToQueueConsumer<T>> toQueue){
    super(context, eventBus, address, localOnly, toQueue);
  }//new

	@Override void receive (MessageImpl msg){
    if (bus.metrics != null){
      bus.metrics.scheduleMessage(metric, msg.isLocal());
    }
    if (!doReceive(msg)){
      discard(msg);
    }
  }
}
