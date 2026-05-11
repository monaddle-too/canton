// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.daml.executors.TaskExecutionObserver
import com.daml.executors.TaskExecutionObserver.*
import com.daml.executors.executors.NamedExecutionContextExecutorService
import com.digitalasset.canton.BaseTestWordSpec

import java.util.concurrent.*

class TaskExecutionObserverTest extends BaseTestWordSpec {

  "NamedExecutionContextExecutorService task observation" should {
    "record queue, run, and worker-thread data for executed runnables" in {
      val observer = new RecordingTaskObserver
      val executor = observedExecutor(observer)
      val ran = new CountDownLatch(1)

      try {
        executor.execute(() => ran.countDown())

        ran.await(5, TimeUnit.SECONDS) shouldBe true
        val completed = observer.nextCompleted()

        completed.started.submitted.executorName shouldBe "observed-executor"
        completed.started.submitted.taskKind shouldBe TaskKind.ExecuteRunnable
        completed.started.queueTimeNanos should be >= 0L
        completed.runTimeNanos should be >= 0L
        completed.started.workerThreadName should startWith("observed-executor-")
        completed.started.workerThreadId should not be Thread.currentThread().getId
        completed.failure shouldBe None
      } finally {
        val _ = executor.shutdownNow()
      }
    }

    "record task failures for submitted callables" in {
      val observer = new RecordingTaskObserver
      val executor = observedExecutor(observer)
      val expected = new IllegalStateException("boom")

      try {
        val future = executor.submit(new Callable[Unit] {
          override def call(): Unit = throw expected
        })

        intercept[ExecutionException](future.get())
        val completed = observer.nextCompleted()

        completed.started.submitted.taskKind shouldBe TaskKind.SubmitCallable
        completed.failure shouldBe Some(expected)
      } finally {
        val _ = executor.shutdownNow()
      }
    }
  }

  "singleThreadScheduledExecutor task observation" should {
    "record queue, run, and worker-thread data for scheduled runnables" in {
      val observer = new RecordingTaskObserver
      val executor = Threading.singleThreadScheduledExecutor(
        "observed-scheduler",
        noTracingLogger,
        observer,
      )
      val ran = new CountDownLatch(1)

      try {
        executor.schedule((() => ran.countDown()): Runnable, 1L, TimeUnit.MILLISECONDS)

        ran.await(5, TimeUnit.SECONDS) shouldBe true
        val completed = observer.nextCompleted()

        completed.started.submitted.executorName shouldBe "observed-scheduler"
        completed.started.submitted.taskKind shouldBe TaskKind.ScheduleRunnable
        completed.started.queueTimeNanos should be >= 0L
        completed.runTimeNanos should be >= 0L
        completed.started.workerThreadName should startWith("observed-scheduler-")
        completed.started.workerThreadId should not be Thread.currentThread().getId
        completed.failure shouldBe None
      } finally {
        val _ = executor.shutdownNow()
      }
    }
  }

  private def observedExecutor(
      observer: TaskExecutionObserver
  ): NamedExecutionContextExecutorService = {
    val delegate = Executors.newSingleThreadExecutor(new ThreadFactory {
      private val counter = new java.util.concurrent.atomic.AtomicInteger(0)

      override def newThread(runnable: Runnable): Thread =
        new Thread(runnable, s"observed-executor-${counter.incrementAndGet()}")
    })
    new NamedExecutionContextExecutorService(
      delegate,
      "observed-executor",
      throwable => logger.error("Observed executor failed", throwable),
      observer,
    )
  }

  private final class RecordingTaskObserver extends TaskExecutionObserver {
    private val completed = new LinkedBlockingQueue[TaskCompleted]()

    override def taskSubmitted(event: TaskSubmitted): TaskHandle = new TaskHandle {
      override def taskStarted(event: TaskStarted): RunningTask = new RunningTask {
        override def taskCompleted(event: TaskCompleted): Unit =
          completed.put(event)
      }

      override def taskRejected(event: TaskRejected): Unit = ()
    }

    def nextCompleted(): TaskCompleted = {
      val event = completed.poll(5, TimeUnit.SECONDS)
      event should not be null
      event
    }
  }
}
