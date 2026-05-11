// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.executors.TaskExecutionObserver
import com.daml.executors.TaskExecutionObserver.*
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.AttributeKey.{booleanKey, longKey, stringKey}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span

/** Propagates the captured [[TraceContext]] onto executor worker threads and emits task timing
  * events on the active span.
  */
final class TraceContextTaskObserver(
    captureTraceContext: () => TraceContext = () => TraceContextGrpc.fromGrpcContext
) extends TaskExecutionObserver {

  override def taskSubmitted(event: TaskSubmitted): TaskHandle = {
    val traceContext = captureTraceContext()
    val span = Option(Span.fromContextOrNull(traceContext.context))
      .filter(_.getSpanContext.isValid)

    span.fold(TaskExecutionObserver.NoOp.taskSubmitted(event)) { span =>
      new TaskHandle {
        override def taskStarted(event: TaskStarted): RunningTask = {
          val scope = traceContext.context.makeCurrent()
          new RunningTask {
            override def taskCompleted(event: TaskCompleted): Unit =
              try {
                val _ = span.addEvent(TraceContextTaskObserver.TaskEventName, taskAttributes(event))
              } finally {
                scope.close()
              }
          }
        }

        override def taskRejected(event: TaskRejected): Unit =
          {
            val _ = span.addEvent(
              TraceContextTaskObserver.TaskRejectedEventName,
              rejectedAttributes(event),
            )
          }
      }
    }
  }

  private def taskAttributes(event: TaskCompleted): Attributes = {
    val submitted = event.started.submitted
    val builder = Attributes
      .builder()
      .put(TraceContextTaskObserver.ExecutorNameKey, submitted.executorName)
      .put(TraceContextTaskObserver.TaskKindKey, submitted.taskKind.name)
      .put(
        TraceContextTaskObserver.SubmitterThreadIdKey,
        java.lang.Long.valueOf(submitted.submitterThreadId),
      )
      .put(TraceContextTaskObserver.SubmitterThreadNameKey, submitted.submitterThreadName)
      .put(
        TraceContextTaskObserver.WorkerThreadIdKey,
        java.lang.Long.valueOf(event.started.workerThreadId),
      )
      .put(TraceContextTaskObserver.WorkerThreadNameKey, event.started.workerThreadName)
      .put(
        TraceContextTaskObserver.QueueTimeNanosKey,
        java.lang.Long.valueOf(event.started.queueTimeNanos),
      )
      .put(TraceContextTaskObserver.RunTimeNanosKey, java.lang.Long.valueOf(event.runTimeNanos))
      .put(TraceContextTaskObserver.FailedKey, java.lang.Boolean.valueOf(event.failure.isDefined))

    event.cpuTimeNanos.foreach(cpuTimeNanos =>
      builder.put(TraceContextTaskObserver.CpuTimeNanosKey, java.lang.Long.valueOf(cpuTimeNanos))
    )
    event.failure.foreach { throwable =>
      builder
        .put(TraceContextTaskObserver.FailureTypeKey, throwable.getClass.getName)
        .put(TraceContextTaskObserver.FailureMessageKey, Option(throwable.getMessage).getOrElse(""))
    }

    builder.build()
  }

  private def rejectedAttributes(event: TaskRejected): Attributes =
    Attributes
      .builder()
      .put(TraceContextTaskObserver.ExecutorNameKey, event.submitted.executorName)
      .put(TraceContextTaskObserver.TaskKindKey, event.submitted.taskKind.name)
      .put(
        TraceContextTaskObserver.SubmitterThreadIdKey,
        java.lang.Long.valueOf(event.submitted.submitterThreadId),
      )
      .put(TraceContextTaskObserver.SubmitterThreadNameKey, event.submitted.submitterThreadName)
      .put(TraceContextTaskObserver.FailureTypeKey, event.failure.getClass.getName)
      .put(
        TraceContextTaskObserver.FailureMessageKey,
        Option(event.failure.getMessage).getOrElse(""),
      )
      .build()
}

object TraceContextTaskObserver {
  val TaskEventName = "canton.executor.task"
  val TaskRejectedEventName = "canton.executor.task.rejected"

  val ExecutorNameKey: AttributeKey[String] = stringKey("canton.executor.name")
  val TaskKindKey: AttributeKey[String] = stringKey("canton.executor.task.kind")
  val SubmitterThreadIdKey: AttributeKey[java.lang.Long] =
    longKey("canton.executor.task.submitter_thread.id")
  val SubmitterThreadNameKey: AttributeKey[String] =
    stringKey("canton.executor.task.submitter_thread.name")
  val WorkerThreadIdKey: AttributeKey[java.lang.Long] =
    longKey("canton.executor.task.worker_thread.id")
  val WorkerThreadNameKey: AttributeKey[String] =
    stringKey("canton.executor.task.worker_thread.name")
  val QueueTimeNanosKey: AttributeKey[java.lang.Long] =
    longKey("canton.executor.task.queue_time_nanos")
  val RunTimeNanosKey: AttributeKey[java.lang.Long] =
    longKey("canton.executor.task.run_time_nanos")
  val CpuTimeNanosKey: AttributeKey[java.lang.Long] =
    longKey("canton.executor.task.cpu_time_nanos")
  val FailedKey: AttributeKey[java.lang.Boolean] = booleanKey("canton.executor.task.failed")
  val FailureTypeKey: AttributeKey[String] = stringKey("canton.executor.task.failure.type")
  val FailureMessageKey: AttributeKey[String] =
    stringKey("canton.executor.task.failure.message")
}
