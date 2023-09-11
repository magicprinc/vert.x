package io.vertx.core.eventbus.impl;

import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.function.BiConsumer;

/**
 * This class is optimized for performance when used on the same event loop it was created on.
 * However it can be used safely from other threads.
 *
 * The internal state is protected using the synchronized keyword. If always used on the same event loop, then
 * we benefit from biased locking which makes the overhead of synchronized near zero.

 @see MessageConsumerImpl
 @see ReplyHandler
 @see RawMessageToQueueConsumer
 */
public class MessageToQueueConsumer<T> extends HandlerRegistration<T> {

  private static final Logger log = LoggerFactory.getLogger(MessageToQueueConsumer.class);

  private final BiConsumer<Message<T>,MessageToQueueConsumer<T>> toQueue;

  public MessageToQueueConsumer (ContextInternal context, EventBusImpl eventBus, String address, boolean localOnly, BiConsumer<Message<T>,MessageToQueueConsumer<T>> toQueue){
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
    dispatchUsingInboundDeliveryContext(message, context);// InboundDeliveryContext will be created and all inboundInterceptors processed
    return true;// BUT: -1) always accept message, event is queue is full  -2) more calls and "layers"
  }

  @Override protected void dispatch (Message<T> msg, ContextInternal contextIsEqThisContext){
    try {
      toQueue.accept(msg, this);
    } catch (Throwable t){
      context.reportException(t);
    }
  }

  public BiConsumer<Message<T>,MessageToQueueConsumer<T>> getHandler (){
    return toQueue;
  }
}
