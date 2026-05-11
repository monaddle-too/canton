// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.tracing.{
  TestTelemetry,
  TestTelemetrySetup,
  TraceContext,
  TraceContextGrpc,
  TraceContextTaskObserver,
}
import io.opentelemetry.context.Context
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.{Callable, TimeUnit}

class EnvironmentObservabilityTest
    extends AnyWordSpec
    with CommunityEnvironmentFixture
    with Eventually {

  "Environment observability" should {
    "install executor task attribution when enabled by tracing config" in new TestEnvironment {
      override def config: CantonConfig =
        sampleConfig.copy(
          monitoring = sampleConfig.monitoring.copy(
            tracing = sampleConfig.monitoring.tracing.copy(
              observability = sampleConfig.monitoring.tracing.observability.copy(
                executorTaskSpanEvents = true
              )
            )
          )
        )

      val telemetry = new TestTelemetrySetup()
      try {
        val rootSpan = telemetry.tracer.spanBuilder("environment-root").startSpan()
        val traceContext = TraceContext.withOpenTelemetryContext(Context.root().`with`(rootSpan))(
          identity
        )

        TraceContextGrpc.withGrpcContext(traceContext) {
          environment.executionContext
            .submit(new Callable[Unit] {
              override def call(): Unit = ()
            })
            .get(5, TimeUnit.SECONDS)
        }
        rootSpan.end()

        eventually(telemetry.reportedSpans() should have size 1)
        val taskEvents = TestTelemetry
          .eventsOrderedByTime(telemetry.reportedSpans()*)
          .filter(_.getName == TraceContextTaskObserver.TaskEventName)
        taskEvents should have size 1
        taskEvents.head.getAttributes.get(
          TraceContextTaskObserver.ExecutorNameKey
        ) should include("-env-ec")
      } finally {
        telemetry.close()
        environment.close()
      }
    }
  }
}
