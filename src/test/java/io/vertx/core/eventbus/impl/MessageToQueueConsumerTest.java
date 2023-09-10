package io.vertx.core.eventbus.impl;


import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 @author Andrey Fink 2023-09-06
 @see MessageToQueueConsumer */
public class MessageToQueueConsumerTest {
  Vertx vertx;
  EventBus eb;

  @Before public void setUp () throws Exception{
    vertx = Vertx.vertx();
    eb = vertx.eventBus();
  }

  @After public void tearDown () throws Exception{
    vertx.close();
    eb = null;  vertx = null;
  }

  @Test public void basic () throws ExecutionException, InterruptedException{
    var sender = eb.sender("kaka.foo");
    assertEquals("kaka.foo", sender.address());

    var msgQueue = new LinkedBlockingDeque<Message<JsonObject>>();

    new MessageToQueueConsumer<>((ContextInternal) vertx.getOrCreateContext(), (EventBusImpl) eb,
      "kaka.foo", false, (Message<JsonObject> m)->msgQueue.offer(m));

    sender.write(JsonObject.of("msg", "Hi!")).toCompletionStage().toCompletableFuture().get();
    sender.write(JsonObject.of("msg", "there")).toCompletionStage().toCompletableFuture().get();
    assertEquals(2, msgQueue.size());
  }

  static final int THREADS = 14; // < 1.2G RAM
  static final int MAX = 1_000_000*THREADS;

  @Test public void testHighload () throws InterruptedException{
    var cnt = new AtomicInteger();
    var inFlight = new AtomicBoolean();
    var numbers = new BitSet(MAX);

    Consumer<Message<Integer>> add = m -> {
      if (inFlight.get()){
        fail("concurrent call");
      }
      inFlight.set(true);
      int i = m.body();
      assertFalse(numbers.get(i));
      numbers.set(i);
      cnt.incrementAndGet();
      inFlight.set(false);
    };

    var consumer = new MessageToQueueConsumer<>((ContextInternal) vertx.getOrCreateContext(), (EventBusImpl) eb,
      "MessageConsumerImplTest.testHighload", false, add);

    assertSame(add, consumer.getHandler());
    assertTrue(consumer.isRegistered());

    var pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(THREADS);
    var startSignal = new CountDownLatch(1);

    IntStream.range(0, THREADS).forEach(idx ->pool.execute(()->{
      try { startSignal.await(); } catch (InterruptedException ignore){}

      for (int i = 0; i<1_000_000; i++){
        int n = idx * 1_000_000 + i;
        eb.send("MessageConsumerImplTest.testHighload", n);
      }
    }));

    Thread.sleep(200);// Time to start all threads. Not scientific, but less code than second CountDownLatch
    long t = System.nanoTime();
    startSignal.countDown();

    var runtime = Runtime.getRuntime();
    runtime.gc();
    System.out.println("Max memory: "+runtime.maxMemory()/1024/1024+", total = "+runtime.totalMemory()/1024/1024);

    while (cnt.get() < MAX){
      System.out.println("q = "+cnt+"\t mem = "+(runtime.totalMemory()-runtime.freeMemory())/1024/1024);
      Thread.sleep(500);
      runtime.gc();// low -Xmx => clean tmp objs
    }

    t = System.nanoTime() - t;

    System.out.println("q is full :-)");

    assertEquals(0, pool.getActiveCount());
    assertEquals(THREADS, pool.getCompletedTaskCount());
    pool.shutdownNow();

    assertEquals(MAX, cnt.get());
    assertEquals(MAX, numbers.size());

    System.out.println("Msg/sec = "+MAX*1_000_000_000L/t);
  }

  @Test public void testHighload2 () throws InterruptedException{
    var cnt = new AtomicInteger();
    var numbers = new BitSet(MAX);

    Consumer<Message<Integer>> add = m -> {
      int i = m.body();
      if (i < 10){
        System.out.println(Thread.currentThread().getName());
      }
      synchronized(numbers){
        assertFalse(numbers.get(i));
        numbers.set(i);
      }
      cnt.incrementAndGet();
    };

    new MessageToQueueConsumer<>((ContextInternal) vertx.getOrCreateContext(), (EventBusImpl) eb,
      "MessageConsumerImplTest.testHighload", false, add);
    // with getOrCreateContext → same thread
    new MessageToQueueConsumer<>(((VertxImpl) vertx).createEventLoopContext(), (EventBusImpl) eb,
      "MessageConsumerImplTest.testHighload", false, add);


    var pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(THREADS);
    var startSignal = new CountDownLatch(1);

    IntStream.range(0, THREADS).forEach(idx ->pool.execute(()->{
      try { startSignal.await(); } catch (InterruptedException ignore){}

      for (int i = 0; i<1_000_000; i++){
        int n = idx * 1_000_000 + i;
        eb.send("MessageConsumerImplTest.testHighload", n);
      }
    }));

    Thread.sleep(200);// Time to start all threads. Not scientific, but less code than second CountDownLatch
    long t = System.nanoTime();
    startSignal.countDown();

    var runtime = Runtime.getRuntime();
    runtime.gc();
    System.out.println("Max memory: "+runtime.maxMemory()/1024/1024+", total = "+runtime.totalMemory()/1024/1024);

    while (cnt.get() < MAX){
      System.out.println("q = "+cnt+"\t mem = "+(runtime.totalMemory()-runtime.freeMemory())/1024/1024);
      Thread.sleep(500);
      runtime.gc();// low -Xmx => clean tmp objs
    }

    t = System.nanoTime() - t;

    System.out.println("q is full :-)");

    assertEquals(0, pool.getActiveCount());
    assertEquals(THREADS, pool.getCompletedTaskCount());
    pool.shutdownNow();

    assertEquals(MAX, cnt.get());
    assertEquals(MAX, numbers.size());

    System.out.println("Msg/sec = "+MAX*1_000_000_000L/t);
  }
}
