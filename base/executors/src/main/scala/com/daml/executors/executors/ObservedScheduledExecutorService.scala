// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors.executors

import com.daml.executors.TaskExecutionObserver
import com.daml.executors.TaskExecutionObserver.*

import java.lang.management.ManagementFactory
import java.util
import java.util.concurrent.*
import scala.util.control.NonFatal

class ObservedScheduledExecutorService(
    private val delegate: ScheduledExecutorService,
    override val name: String,
    taskObserver: TaskExecutionObserver,
) extends ScheduledExecutorService
    with NamedExecutor {

  override def shutdown(): Unit = delegate.shutdown()
  override def shutdownNow(): util.List[Runnable] = delegate.shutdownNow()
  override def isShutdown: Boolean = delegate.isShutdown
  override def isTerminated: Boolean = delegate.isTerminated
  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    delegate.awaitTermination(timeout, unit)

  override def execute(command: Runnable): Unit = {
    val instrumented = instrument(command, TaskKind.ExecuteRunnable)
    try delegate.execute(instrumented.task)
    catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }
  }

  override def submit[T](task: Callable[T]): Future[T] = {
    val instrumented = instrument(task, TaskKind.SubmitCallable)
    try delegate.submit(instrumented.task)
    catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }
  }

  override def submit[T](task: Runnable, result: T): Future[T] = {
    val instrumented = instrument(task, TaskKind.SubmitRunnable)
    try delegate.submit(instrumented.task, result)
    catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }
  }

  override def submit(task: Runnable): Future[?] = {
    val instrumented = instrument(task, TaskKind.SubmitRunnable)
    try delegate.submit(instrumented.task)
    catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }
  }

  override def invokeAll[T](tasks: util.Collection[? <: Callable[T]]): util.List[Future[T]] =
    delegate.invokeAll(instrumentAll(tasks))

  override def invokeAll[T](
      tasks: util.Collection[? <: Callable[T]],
      timeout: Long,
      unit: TimeUnit,
  ): util.List[Future[T]] =
    delegate.invokeAll(instrumentAll(tasks), timeout, unit)

  override def invokeAny[T](tasks: util.Collection[? <: Callable[T]]): T =
    delegate.invokeAny(instrumentAll(tasks))

  override def invokeAny[T](
      tasks: util.Collection[? <: Callable[T]],
      timeout: Long,
      unit: TimeUnit,
  ): T =
    delegate.invokeAny(instrumentAll(tasks), timeout, unit)

  override def schedule(
      command: Runnable,
      delay: Long,
      unit: TimeUnit,
  ): ScheduledFuture[?] = {
    val instrumented = instrument(command, TaskKind.ScheduleRunnable)
    try delegate.schedule(instrumented.task, delay, unit)
    catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }
  }

  override def schedule[V](
      callable: Callable[V],
      delay: Long,
      unit: TimeUnit,
  ): ScheduledFuture[V] = {
    val instrumented = instrument(callable, TaskKind.ScheduleCallable)
    try delegate.schedule(instrumented.task, delay, unit)
    catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }
  }

  override def scheduleAtFixedRate(
      command: Runnable,
      initialDelay: Long,
      period: Long,
      unit: TimeUnit,
  ): ScheduledFuture[?] =
    delegate.scheduleAtFixedRate(
      recurring(command, TaskKind.ScheduleAtFixedRateRunnable),
      initialDelay,
      period,
      unit,
    )

  override def scheduleWithFixedDelay(
      command: Runnable,
      initialDelay: Long,
      delay: Long,
      unit: TimeUnit,
  ): ScheduledFuture[?] =
    delegate.scheduleWithFixedDelay(
      recurring(command, TaskKind.ScheduleWithFixedDelayRunnable),
      initialDelay,
      delay,
      unit,
    )

  private def instrument(runnable: Runnable, taskKind: TaskKind): Instrumented[Runnable] = {
    val submitted = taskSubmitted(taskKind)
    val handle = new ObservedTaskHandle(submitted, taskObserver.taskSubmitted(submitted))
    new Instrumented(new ObservedRunnable(runnable, handle), handle)
  }

  private def instrument[T](
      callable: Callable[T],
      taskKind: TaskKind,
  ): Instrumented[Callable[T]] = {
    val submitted = taskSubmitted(taskKind)
    val handle = new ObservedTaskHandle(submitted, taskObserver.taskSubmitted(submitted))
    new Instrumented(new ObservedCallable(callable, handle), handle)
  }

  private def recurring(runnable: Runnable, taskKind: TaskKind): Runnable =
    () => {
      val instrumented = instrument(runnable, taskKind)
      instrumented.task.run()
    }

  private def instrumentAll[T](
      tasks: util.Collection[? <: Callable[T]]
  ): util.Collection[? <: Callable[T]] = {
    val instrumented = new util.ArrayList[Callable[T]](tasks.size())
    val iterator = tasks.iterator()
    while (iterator.hasNext) {
      val callable = iterator.next()
      instrumented.add(instrument(callable, TaskKind.SubmitCallable).task)
    }
    instrumented
  }

  private def taskSubmitted(taskKind: TaskKind): TaskSubmitted = {
    val thread = Thread.currentThread()
    TaskSubmitted(
      executorName = name,
      taskKind = taskKind,
      submittedAtNanos = System.nanoTime(),
      submitterThreadId = thread.getId,
      submitterThreadName = thread.getName,
    )
  }

  private def taskRejected(handle: ObservedTaskHandle, failure: Throwable): TaskRejected =
    TaskRejected(
      submitted = handle.submitted,
      rejectedAtNanos = System.nanoTime(),
      failure = failure,
    )

  private final class Instrumented[A](val task: A, val handle: ObservedTaskHandle)

  private final class ObservedTaskHandle(
      val submitted: TaskSubmitted,
      delegate: TaskHandle,
  ) extends TaskHandle {
    override def taskStarted(event: TaskStarted): RunningTask = delegate.taskStarted(event)
    override def taskRejected(event: TaskRejected): Unit = delegate.taskRejected(event)
  }

  private final class ObservedRunnable(delegate: Runnable, handle: ObservedTaskHandle)
      extends Runnable {
    override def run(): Unit = runObserved(handle)(delegate.run())
  }

  private final class ObservedCallable[T](delegate: Callable[T], handle: ObservedTaskHandle)
      extends Callable[T] {
    override def call(): T = runObserved(handle)(delegate.call())
  }

  private def runObserved[T](handle: ObservedTaskHandle)(body: => T): T = {
    val startedAtNanos = System.nanoTime()
    val thread = Thread.currentThread()
    val submitted = handle.submitted
    val started = TaskStarted(
      submitted = submitted,
      startedAtNanos = startedAtNanos,
      queueTimeNanos = nonNegativeNanos(startedAtNanos - submitted.submittedAtNanos),
      workerThreadId = thread.getId,
      workerThreadName = thread.getName,
    )
    val running = handle.taskStarted(started)
    val startedCpuTimeNanos = currentThreadCpuTimeNanos
    var failure: Throwable = null
    try {
      body
    } catch {
      case throwable: Throwable =>
        failure = throwable
        throw throwable
    } finally {
      val completedAtNanos = System.nanoTime()
      running.taskCompleted(
        TaskCompleted(
          started = started,
          completedAtNanos = completedAtNanos,
          runTimeNanos = nonNegativeNanos(completedAtNanos - startedAtNanos),
          cpuTimeNanos = cpuTimeElapsedNanos(startedCpuTimeNanos),
          failure = Option(failure),
        )
      )
    }
  }

  private def currentThreadCpuTimeNanos: Option[Long] =
    Option
      .when(ObservedScheduledCpuTime.threadCpuTimeAvailable) {
        ObservedScheduledCpuTime.threadMxBean.getCurrentThreadCpuTime
      }
      .filter(_ >= 0L)

  private def cpuTimeElapsedNanos(start: Option[Long]): Option[Long] =
    for {
      startNanos <- start
      completedNanos <- currentThreadCpuTimeNanos
    } yield nonNegativeNanos(completedNanos - startNanos)

  private def nonNegativeNanos(value: Long): Long = Math.max(0L, value)
}

private object ObservedScheduledCpuTime {
  val threadMxBean = ManagementFactory.getThreadMXBean
  val threadCpuTimeAvailable: Boolean =
    threadMxBean.isCurrentThreadCpuTimeSupported && threadMxBean.isThreadCpuTimeEnabled
}
