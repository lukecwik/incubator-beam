/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.LongMath;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.KeyForBottom;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An unbounded {@link ScheduledExecutorService} based upon the {@link ScheduledThreadPoolExecutor}
 * API contract.
 *
 * <p>Note that this implementation differs from a {@link ScheduledThreadPoolExecutor} in the
 * following ways:
 *
 * <ul>
 *   <li>The core pool size is always 0.
 *   <li>Any work that is immediately executable is given to a thread before returning from the
 *       corresponding {@code execute}, {@code submit}, {@code schedule*} methods.
 *   <li>An unbounded number of threads can be started.
 * </ul>
 */
public final class UnboundedScheduledExecutorService implements ScheduledExecutorService {
  /**
   * A {@link FutureTask} that handles periodically rescheduling tasks.
   *
   * <p>Note that it is important that this class extends {@link FutureTask} and {@link
   * RunnableScheduledFuture} to be compatible with the types of objects returned by a {@link
   * ScheduledThreadPoolExecutor}.
   */
  @VisibleForTesting
  @SuppressFBWarnings(
      value = "EQ_COMPARETO_USE_OBJECT_EQUALS",
      justification =
          "Default equals/hashCode is what we want since two scheduled tasks are only equivalent if they point to the same instance.")
  final class ScheduledFutureTask<@Nullable @KeyForBottom V> extends FutureTask<V>
      implements RunnableScheduledFuture<V> {

    /** Sequence number to break ties FIFO. */
    private final long sequenceNumber;

    /** The time the task is enabled to execute in nanoTime units. */
    private long time;

    /**
     * Period in nanoseconds for repeating tasks. A positive value indicates fixed-rate execution. A
     * negative value indicates fixed-delay execution. A value of 0 indicates a non-repeating
     * (one-shot) task.
     */
    private final long period;

    /** Creates a one-shot action with given nanoTime-based trigger time. */
    ScheduledFutureTask(Runnable r, @Nullable V result, long triggerTime) {
      this(r, result, triggerTime, 0);
    }

    /** Creates a periodic action with given nanoTime-based initial trigger time and period. */
    @SuppressWarnings("argument")
    ScheduledFutureTask(Runnable r, @Nullable V result, long triggerTime, long period) {
      super(r, result);
      this.time = triggerTime;
      this.period = period;
      this.sequenceNumber = sequencer.getAndIncrement();
    }

    /** Creates a one-shot action with given nanoTime-based trigger time. */
    ScheduledFutureTask(Callable<V> callable, long triggerTime) {
      super(callable);
      this.time = triggerTime;
      this.period = 0;
      this.sequenceNumber = sequencer.getAndIncrement();
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(LongMath.saturatedSubtract(time, clock.nanoTime()), NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      if (other == this) // compare zero if same object
      {
        return 0;
      }
      if (other instanceof ScheduledFutureTask) {
        ScheduledFutureTask<?> x = (ScheduledFutureTask<?>) other;
        int diff = Longs.compare(time, x.time);
        if (diff != 0) {
          return diff;
        }
        if (sequenceNumber < x.sequenceNumber) {
          return -1;
        }
        return 1;
      }
      long diff = LongMath.saturatedSubtract(getDelay(NANOSECONDS), other.getDelay(NANOSECONDS));
      return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
    }

    @Override
    public boolean isPeriodic() {
      return period != 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      synchronized (tasks) {
        tasks.remove(this);
      }
      return cancelled;
    }

    /** Overrides {@link FutureTask} so as to reset/requeue if periodic. */
    @Override
    public void run() {
      boolean periodic = isPeriodic();
      if (!periodic) {
        super.run();
      } else if (super.runAndReset()) {
        // Set the next runtime
        if (period > 0) {
          time = LongMath.saturatedAdd(time, period);
        } else {
          time = triggerTime(-period);
        }
        synchronized (tasks) {
          tasks.add(this);
          tasks.notify();
        }
      }
    }

    @Override
    public String toString() {
      return "ScheduledFutureTask[nextScheduledTime=" + time + ", period=" + period + "]";
    }
  }

  // Used to break ties in ordering of future tasks that are scheduled for the same time
  // so that they have a consistent ordering based upon their insertion order into
  // this ScheduledExecutorService.
  private final AtomicLong sequencer = new AtomicLong();

  private final NanoClock clock;
  private final AtomicBoolean isShutdown;
  private final AtomicBoolean isTerminated;
  private final ThreadFactory threadFactory;
  @VisibleForTesting final Set<Worker> allWorkers;
  private final ConcurrentLinkedDeque<Worker> pendingWorkers;
  @VisibleForTesting final PriorityQueue<ScheduledFutureTask<?>> tasks;
  private final AbstractExecutorService invokeMethodsAdapter;
  private final Future<?> launchTasks;

  public UnboundedScheduledExecutorService() {
    this(NanoClock.SYSTEM);
  }

  private class Worker implements Runnable {
    private final Thread thread;
    private Runnable runnable;

    @SuppressWarnings("argument") // the thread is started outside of this worker
    private Worker(Runnable runnable) {
      this.runnable = runnable;
      this.thread = threadFactory.newThread(this);
    }

    @Override
    public void run() {
      try {
        while (true) {
          runnable.run();
          synchronized (this) {
            pendingWorkers.add(this);
            try {
              this.wait();
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      } finally {
        synchronized (allWorkers) {
          allWorkers.remove(this);
        }
      }
    }
  }

  @SuppressWarnings({
    "argument",
    "method.invocation"
  }) // synchronized for safe object publishing for TaskLauncher
  @VisibleForTesting
  UnboundedScheduledExecutorService(NanoClock clock) {
    this.clock = clock;
    this.isShutdown = new AtomicBoolean();
    this.isTerminated = new AtomicBoolean();
    this.threadFactory =
        new ThreadFactoryBuilder()
            .setThreadFactory(MoreExecutors.platformThreadFactory())
            .setDaemon(true)
            .build();
    this.allWorkers = new HashSet<>();
    this.pendingWorkers = new ConcurrentLinkedDeque<>();

    // Create an internal adapter so that execute does not re-wrap the ScheduledFutureTask again
    this.invokeMethodsAdapter =
        new AbstractExecutorService() {

          @Override
          protected <@KeyForBottom T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
            return new ScheduledFutureTask<>(runnable, value, 0);
          }

          @Override
          protected <@KeyForBottom T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new ScheduledFutureTask<>(callable, 0);
          }

          @Override
          public void shutdown() {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isShutdown() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isTerminated() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
          }

          @Override
          /* UnboundedScheduledExecutorService is the only caller after it has been initialized.*/
          @SuppressWarnings("method.invocation")
          public void execute(Runnable command) {
            runNow(command);
          }
        };
    this.tasks = new PriorityQueue<>();

    synchronized (tasks) {
      // Ensure that we synchronize on tasks to ensure that we are able to finish the
      // constructor and publish the object safely since TaskLauncher also synchronizes on tasks
      // before performing any lookups.
      this.launchTasks = submit(new TaskLauncher());
    }
  }

  private class TaskLauncher implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      while (true) {
        synchronized (tasks) {
          if (isShutdown.get()) {
            return null;
          }
          ScheduledFutureTask<?> task = tasks.peek();
          if (task == null) {
            tasks.wait();
            continue;
          }
          long nanosToWait = LongMath.saturatedSubtract(task.time, clock.nanoTime());
          if (nanosToWait > 0) {
            long millisToWait = nanosToWait / 1_000_000;
            int nanosRemainder = (int) (nanosToWait % 1_000_000);
            tasks.wait(millisToWait, nanosRemainder);
            continue;
          }
          // Remove the task from the queue since it is ready to be scheduled now
          task = tasks.remove();
          runNow(task);
        }
      }
    }
  }

  @Override
  public void shutdown() {
    isShutdown.set(true);
    synchronized (tasks) {
      // Notify tasks which causes the TaskLauncher to see if the ThreadPoolExecutor is shutdown and
      // exits cleanly.
      tasks.notify();
    }

    // Re-throw any errors during shutdown of the launchTasks thread.
    try {
      launchTasks.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown();

    if (isTerminated.get()) {
      return Collections.emptyList();
    }

    synchronized (allWorkers) {
      for (Worker worker : allWorkers) {
        worker.thread.interrupt();
      }
      allWorkers.clear();
    }

    synchronized (tasks) {
      List<Runnable> rval = new ArrayList<>(tasks);
      tasks.clear();
      return rval;
    }
  }

  @Override
  public boolean isShutdown() {
    return isShutdown.get();
  }

  @Override
  public boolean isTerminated() {
    return isTerminated.get();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long deadline = LongMath.saturatedAdd(System.nanoTime(), unit.toNanos(timeout));
    synchronized (allWorkers) {
      while (!allWorkers.isEmpty()) {
        long remainingTimeToWait = LongMath.saturatedSubtract(deadline, System.nanoTime());
        if (remainingTimeToWait <= 0) {
          return false;
        }
        allWorkers.wait(remainingTimeToWait / 1000000L, (int) (remainingTimeToWait % 1000000L));
      }
      return true;
    }
  }

  @Override
  public void execute(Runnable command) {
    if (command == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<Void> task = new ScheduledFutureTask<>(command, null, triggerTime(0));
    runNow(task);
  }

  @Override
  public Future<@Nullable ?> submit(Runnable command) {
    if (command == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<Void> task = new ScheduledFutureTask<>(command, null, triggerTime(0));
    runNow(task);
    return task;
  }

  @Override
  /* Ignore improper flag since FB detects that ScheduledExecutorService can't have nullable V. */
  @SuppressWarnings("override.return")
  public <@Nullable @KeyForBottom T> Future<T> submit(Runnable command, T result) {
    if (command == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<T> task = new ScheduledFutureTask<>(command, result, triggerTime(0));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  @Override
  /* Ignore improper flag since FB detects that ScheduledExecutorService can't have nullable V. */
  @SuppressWarnings({"override.param", "override.return"})
  public <@Nullable @KeyForBottom T> Future<T> submit(Callable<T> command) {
    if (command == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<T> task = new ScheduledFutureTask<>(command, triggerTime(0));
    runNow(task);
    return task;
  }

  @Override
  public <@KeyForBottom T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return invokeMethodsAdapter.invokeAll(tasks);
  }

  @Override
  public <@KeyForBottom T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (tasks == null || unit == null) {
      throw new NullPointerException();
    }
    return invokeMethodsAdapter.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <@KeyForBottom T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return invokeMethodsAdapter.invokeAny(tasks);
  }

  @Override
  public <@KeyForBottom T> T invokeAny(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return invokeMethodsAdapter.invokeAny(tasks, timeout, unit);
  }

  @Override
  public ScheduledFuture<@Nullable ?> schedule(Runnable command, long delay, TimeUnit unit) {
    if (command == null || unit == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<Void> task =
        new ScheduledFutureTask<>(command, null, triggerTime(delay, unit));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  @Override
  /* Ignore improper flag since FB detects that ScheduledExecutorService can't have nullable V. */
  @SuppressWarnings({"override.param", "override.return"})
  public <@Nullable @KeyForBottom V> ScheduledFuture<V> schedule(
      Callable<V> callable, long delay, TimeUnit unit) {
    if (callable == null || unit == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<V> task = new ScheduledFutureTask<>(callable, triggerTime(delay, unit));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  @Override
  public ScheduledFuture<@Nullable ?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    if (command == null || unit == null) {
      throw new NullPointerException();
    }
    if (period <= 0) {
      throw new IllegalArgumentException();
    }
    ScheduledFutureTask<Void> task =
        new ScheduledFutureTask<Void>(
            command, null, triggerTime(initialDelay, unit), unit.toNanos(period));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  @Override
  public ScheduledFuture<@Nullable ?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    if (command == null || unit == null) {
      throw new NullPointerException();
    }
    if (delay <= 0) {
      throw new IllegalArgumentException();
    }
    ScheduledFutureTask<Void> task =
        new ScheduledFutureTask<>(
            command, null, triggerTime(initialDelay, unit), -unit.toNanos(delay));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  private <@Nullable @KeyForBottom T> void runNow(Runnable command) {
    if (isShutdown.get()) {
      throw new RejectedExecutionException("Task " + command + " rejected from " + this);
    }
    Worker worker = pendingWorkers.poll();
    if (worker == null) {
      worker = new Worker(command);
      synchronized (allWorkers) {
        allWorkers.add(worker);
      }
      worker.thread.start();
    } else {
      synchronized (worker) {
        worker.runnable = command;
        worker.notify();
      }
    }
  }

  private <@Nullable @KeyForBottom T> void runNowOrScheduleInTheFuture(
      ScheduledFutureTask<T> task) {
    long nanosToWait = LongMath.saturatedSubtract(task.time, clock.nanoTime());
    if (nanosToWait <= 0) {
      runNow(task);
      return;
    }

    synchronized (tasks) {
      if (isShutdown.get()) {
        throw new RejectedExecutionException("Task " + task + " rejected from " + this);
      }
      tasks.add(task);
      tasks.notify();
    }
  }

  @Override
  public String toString() {
    int numWorkers;

    synchronized (allWorkers) {
      numWorkers = allWorkers.size();
    }
    return "UnboundedScheduledExecutorService[numThreads="
        + numWorkers
        + ", numScheduledTasks="
        + pendingWorkers.size()
        + "]";
  }

  /** Returns the nanoTime-based trigger time of a delayed action. */
  private long triggerTime(long delay, TimeUnit unit) {
    return triggerTime(unit.toNanos((delay < 0) ? 0 : delay));
  }

  /** Returns the nanoTime-based trigger time of a delayed action. */
  private long triggerTime(long delay) {
    return LongMath.saturatedAdd(clock.nanoTime(), delay);
  }
}
