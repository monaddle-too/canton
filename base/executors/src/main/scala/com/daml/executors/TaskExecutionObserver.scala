// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors

/** Observer hook for attributing executor work to queue/run/thread/CPU intervals.
  *
  * Implementations must be cheap and non-blocking; they run on task submission and worker threads.
  */
trait TaskExecutionObserver {
  def taskSubmitted(event: TaskExecutionObserver.TaskSubmitted): TaskExecutionObserver.TaskHandle
}

object TaskExecutionObserver {

  sealed trait TaskKind {
    def name: String
  }

  object TaskKind {
    case object ExecuteRunnable extends TaskKind {
      override val name: String = "execute-runnable"
    }

    case object SubmitRunnable extends TaskKind {
      override val name: String = "submit-runnable"
    }

    case object SubmitCallable extends TaskKind {
      override val name: String = "submit-callable"
    }

    case object ScheduleRunnable extends TaskKind {
      override val name: String = "schedule-runnable"
    }

    case object ScheduleCallable extends TaskKind {
      override val name: String = "schedule-callable"
    }

    case object ScheduleAtFixedRateRunnable extends TaskKind {
      override val name: String = "schedule-at-fixed-rate-runnable"
    }

    case object ScheduleWithFixedDelayRunnable extends TaskKind {
      override val name: String = "schedule-with-fixed-delay-runnable"
    }
  }

  final case class TaskSubmitted(
      executorName: String,
      taskKind: TaskKind,
      submittedAtNanos: Long,
      submitterThreadId: Long,
      submitterThreadName: String,
  )

  final case class TaskStarted(
      submitted: TaskSubmitted,
      startedAtNanos: Long,
      queueTimeNanos: Long,
      workerThreadId: Long,
      workerThreadName: String,
  )

  final case class TaskCompleted(
      started: TaskStarted,
      completedAtNanos: Long,
      runTimeNanos: Long,
      cpuTimeNanos: Option[Long],
      failure: Option[Throwable],
  )

  final case class TaskRejected(
      submitted: TaskSubmitted,
      rejectedAtNanos: Long,
      failure: Throwable,
  )

  trait TaskHandle {
    def taskStarted(event: TaskStarted): RunningTask
    def taskRejected(event: TaskRejected): Unit
  }

  trait RunningTask {
    def taskCompleted(event: TaskCompleted): Unit
  }

  object NoOp extends TaskExecutionObserver {
    override def taskSubmitted(event: TaskSubmitted): TaskHandle = NoOpTaskHandle
  }

  private object NoOpTaskHandle extends TaskHandle {
    override def taskStarted(event: TaskStarted): RunningTask = NoOpRunningTask
    override def taskRejected(event: TaskRejected): Unit = ()
  }

  private object NoOpRunningTask extends RunningTask {
    override def taskCompleted(event: TaskCompleted): Unit = ()
  }
}
