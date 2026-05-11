// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.executors.executors.NamedExecutionContextExecutorService
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.tracing.TestTelemetry.eventsOrderedByTime
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context

import java.util.concurrent.*

class TraceContextTaskObserverTest extends BaseTestWordSpec {

  "TraceContextTaskObserver" should {
    "make the captured trace context current while executor work runs and emit a task event" in {
      val telemetry = new TestTelemetrySetup()
      val rootSpan = telemetry.tracer.spanBuilder("root").startSpan()
      val traceContext = TraceContext(Context.root().`with`(rootSpan))
      val observer = new TraceContextTaskObserver(() => traceContext)
      val executor = {
        val delegate = Executors.newSingleThreadExecutor(new ThreadFactory {
          private val counter = new java.util.concurrent.atomic.AtomicInteger(0)

          override def newThread(runnable: Runnable): Thread =
            new Thread(runnable, s"trace-executor-${counter.incrementAndGet()}")
        })
        new NamedExecutionContextExecutorService(
          delegate,
          "trace-executor",
          throwable => noTracingLogger.error("Trace executor failed", throwable),
          observer,
        )
      }

      try {
        val future = executor.submit(new Callable[Unit] {
          override def call(): Unit =
            Span.current().getSpanContext.getSpanId shouldBe rootSpan.getSpanContext.getSpanId
        })

        future.get(5, TimeUnit.SECONDS)
        rootSpan.end()

        val spans = telemetry.reportedSpans()
        spans should have size 1
        val span = spans.head
        val taskEvents = eventsOrderedByTime(span).filter(
          _.getName == TraceContextTaskObserver.TaskEventName
        )
        taskEvents should have size 1
        val taskEvent = taskEvents.head

        taskEvent.getAttributes.get(TraceContextTaskObserver.ExecutorNameKey) shouldBe
          "trace-executor"
        taskEvent.getAttributes.get(
          TraceContextTaskObserver.WorkerThreadNameKey
        ) should startWith("trace-executor-")
        taskEvent.getAttributes.get(
          TraceContextTaskObserver.QueueTimeNanosKey
        ).longValue() should be >= 0L
        taskEvent.getAttributes.get(
          TraceContextTaskObserver.RunTimeNanosKey
        ).longValue() should be >= 0L
      } finally {
        val _ = executor.shutdownNow()
        telemetry.close()
      }
    }
  }
}
