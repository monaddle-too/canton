// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors.executors

import com.daml.executors.TaskExecutionObserver
import com.daml.executors.TaskExecutionObserver.*

import java.lang.management.ManagementFactory
import java.util
import java.util.concurrent.{Callable, ExecutorService, Future, TimeUnit}
import scala.concurrent.ExecutionContextExecutorService
import scala.util.control.NonFatal

class NamedExecutionContextExecutorService(
    protected val delegate: ExecutorService,
    val name: String,
    reporter: Throwable => Unit,
    taskObserver: TaskExecutionObserver = TaskExecutionObserver.NoOp,
) extends ExecutionContextExecutorService
    with NamedExecutor {

  override def reportFailure(cause: Throwable): Unit = reporter(cause)

  override def shutdown(): Unit = delegate.shutdown()
  override def shutdownNow(): util.List[Runnable] = delegate.shutdownNow()
  override def isShutdown: Boolean = delegate.isShutdown
  override def isTerminated: Boolean = delegate.isTerminated
  override def awaitTermination(l: Long, timeUnit: TimeUnit): Boolean =
    delegate.awaitTermination(l, timeUnit)

  override def submit[T](callable: Callable[T]): Future[T] =
    if (taskObservationEnabled)
      submitCallableInstrumented(instrument(callable, TaskKind.SubmitCallable))
    else delegate.submit(callable)

  override def submit[T](runnable: Runnable, t: T): Future[T] =
    if (taskObservationEnabled)
      submitRunnableInstrumented(instrument(runnable, TaskKind.SubmitRunnable), t)
    else delegate.submit(runnable, t)

  override def submit(runnable: Runnable): Future[?] =
    if (taskObservationEnabled)
      submitRunnableInstrumented(instrument(runnable, TaskKind.SubmitRunnable))
    else delegate.submit(runnable)

  override def invokeAll[T](
      collection: util.Collection[? <: Callable[T]]
  ): util.List[Future[T]] =
    delegate.invokeAll(instrumentAll(collection))

  override def invokeAll[T](
      collection: util.Collection[? <: Callable[T]],
      l: Long,
      timeUnit: TimeUnit,
  ): util.List[Future[T]] =
    delegate.invokeAll(instrumentAll(collection), l, timeUnit)

  override def invokeAny[T](
      collection: util.Collection[? <: Callable[T]]
  ): T =
    delegate.invokeAny(instrumentAll(collection))

  override def invokeAny[T](
      collection: util.Collection[? <: Callable[T]],
      l: Long,
      timeUnit: TimeUnit,
  ): T =
    delegate.invokeAny(instrumentAll(collection), l, timeUnit)

  override def execute(runnable: Runnable): Unit =
    if (taskObservationEnabled) executeInstrumented(instrument(runnable, TaskKind.ExecuteRunnable))
    else delegate.execute(runnable)

  private[this] val taskObservationEnabled: Boolean =
    taskObserver ne TaskExecutionObserver.NoOp

  private[this] def instrument(runnable: Runnable, taskKind: TaskKind): Instrumented[Runnable] = {
    val submitted = taskSubmitted(taskKind)
    val handle = new ObservedTaskHandle(submitted, taskObserver.taskSubmitted(submitted))
    new Instrumented(new ObservedRunnable(runnable, handle), handle)
  }

  private[this] def instrument[T](
      callable: Callable[T],
      taskKind: TaskKind,
  ): Instrumented[Callable[T]] = {
    val submitted = taskSubmitted(taskKind)
    val handle = new ObservedTaskHandle(submitted, taskObserver.taskSubmitted(submitted))
    new Instrumented(new ObservedCallable(callable, handle), handle)
  }

  private[this] def instrumentAll[T](
      collection: util.Collection[? <: Callable[T]]
  ): util.Collection[? <: Callable[T]] =
    if (taskObservationEnabled) {
      val instrumented = new util.ArrayList[Callable[T]](collection.size())
      val iterator = collection.iterator()
      while (iterator.hasNext) {
        val callable = iterator.next()
        instrumented.add(instrument(callable, TaskKind.SubmitCallable).task)
      }
      instrumented
    } else collection

  private[this] def submitCallableInstrumented[T](
      instrumented: Instrumented[Callable[T]]
  ): Future[T] =
    try {
      delegate.submit(instrumented.task)
    } catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }

  private[this] def submitRunnableInstrumented[T](
      instrumented: Instrumented[Runnable],
      result: T,
  ): Future[T] =
    try {
      delegate.submit(instrumented.task, result)
    } catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }

  private[this] def submitRunnableInstrumented(instrumented: Instrumented[Runnable]): Future[?] =
    try {
      delegate.submit(instrumented.task)
    } catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }

  private[this] def executeInstrumented(instrumented: Instrumented[Runnable]): Unit =
    try {
      delegate.execute(instrumented.task)
    } catch {
      case NonFatal(exception) =>
        instrumented.handle.taskRejected(taskRejected(instrumented.handle, exception))
        throw exception
    }

  private[this] def taskSubmitted(taskKind: TaskKind): TaskSubmitted = {
    val thread = Thread.currentThread()
    TaskSubmitted(
      executorName = name,
      taskKind = taskKind,
      submittedAtNanos = System.nanoTime(),
      submitterThreadId = thread.getId,
      submitterThreadName = thread.getName,
    )
  }

  private[this] def taskRejected(handle: ObservedTaskHandle, failure: Throwable): TaskRejected =
    TaskRejected(
      submitted = handle.submitted,
      rejectedAtNanos = System.nanoTime(),
      failure = failure,
    )

  private[this] final class Instrumented[A](val task: A, val handle: ObservedTaskHandle)

  private[this] final class ObservedTaskHandle(
      val submitted: TaskSubmitted,
      delegate: TaskHandle,
  ) extends TaskHandle {
    override def taskStarted(event: TaskStarted): RunningTask = delegate.taskStarted(event)
    override def taskRejected(event: TaskRejected): Unit = delegate.taskRejected(event)
  }

  private[this] final class ObservedRunnable(delegate: Runnable, handle: ObservedTaskHandle)
      extends Runnable {
    override def run(): Unit = runObserved(handle)(delegate.run())
  }

  private[this] final class ObservedCallable[T](delegate: Callable[T], handle: ObservedTaskHandle)
      extends Callable[T] {
    override def call(): T = runObserved(handle)(delegate.call())
  }

  private[this] def runObserved[T](handle: ObservedTaskHandle)(body: => T): T = {
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

  private[this] def currentThreadCpuTimeNanos: Option[Long] =
    Option
      .when(CpuTime.threadCpuTimeAvailable) {
        CpuTime.threadMxBean.getCurrentThreadCpuTime
      }
      .filter(_ >= 0L)

  private[this] def cpuTimeElapsedNanos(start: Option[Long]): Option[Long] =
    for {
      startNanos <- start
      completedNanos <- currentThreadCpuTimeNanos
    } yield nonNegativeNanos(completedNanos - startNanos)

  private[this] def nonNegativeNanos(value: Long): Long = Math.max(0L, value)
}

private object CpuTime {
  val threadMxBean = ManagementFactory.getThreadMXBean
  val threadCpuTimeAvailable: Boolean =
    threadMxBean.isCurrentThreadCpuTimeSupported && threadMxBean.isThreadCpuTimeEnabled
}
