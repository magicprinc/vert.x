package io.vertx.core.eventbus.impl;

import io.vertx.core.Promise;
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
public class RawMessageToQueueConsumer<T> extends HandlerRegistration<T> {

  private static final Logger log = LoggerFactory.getLogger(RawMessageToQueueConsumer.class);

  private final BiPredicate<Message<T>,RawMessageToQueueConsumer<T>> toQueue;

  public RawMessageToQueueConsumer (ContextInternal context, EventBusImpl eventBus, String address, boolean localOnly, BiPredicate<Message<T>,RawMessageToQueueConsumer<T>> toQueue){
    super(context, eventBus, address, false);
    this.toQueue = toQueue;

    Promise<Void> reg = context.promise();
    register(null, localOnly, reg);
    reg.future().onComplete(ar -> {
      if (ar.succeeded()){
        log.debug("registered: "+ address);
      } else {
        log.warn("registration failed: "+ address, ar.cause());
      }
    });
  }//new

  public String address (){
    return address;
  }

  /**
   @see MessageConsumerImpl#doReceive
   @see ReplyHandler#doReceive
   @see HandlerRegistration#dispatchUsingInboundDeliveryContext
   */
  @Override protected boolean doReceive (Message<T> message){
    // × bus.inboundInterceptors(); - they require DeliveryContext :-( too complex ...
    return toQueue.test(message, this);// usually BlockingQueue::offer
  }

  @Override protected void dispatch (Message<T> msg, ContextInternal context){
    // context.dispatch(msg, handler);
    throw new IllegalStateException("dispatch: must not be called: HandlerRegistration.receive → #doReceive → dispatch → #dispatch: "+msg);
  }

  public BiPredicate<Message<T>,RawMessageToQueueConsumer<T>> getHandler (){
    return toQueue;
  }
}
