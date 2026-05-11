// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.executors.executors.NamedExecutionContextExecutorService
import com.digitalasset.canton.networking.grpc.CantonGrpcUtilTest
import com.digitalasset.canton.protobuf.Hello
import com.digitalasset.canton.protobuf.HelloServiceGrpc.HelloService
import com.digitalasset.canton.tracing.TestTelemetry.eventsOrderedByTime
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.stub.StreamObserver
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.context.Context
import org.scalatest.Outcome
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.FixtureAnyWordSpec

import java.util.concurrent.{Callable, CountDownLatch, Executors, ThreadFactory, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class GrpcTraceInstrumentationTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext
    with Eventually {

  override type FixtureParam = Env

  class TestHelloService extends HelloService {
    override def hello(request: Hello.Request): Future[Hello.Response] =
      Future.successful(CantonGrpcUtilTest.response)

    override def helloStreamed(
        request: Hello.Request,
        responseObserver: StreamObserver[Hello.Response],
    ): Unit = {
      responseObserver.onNext(Hello.Response("one"))
      responseObserver.onNext(Hello.Response("two"))
      responseObserver.onCompleted()
    }
  }

  class Env {
    val telemetry = new TestTelemetrySetup()
    val grpc = new CantonGrpcUtilTest.Env(
      new TestHelloService,
      loggerFactory.append("grpc-trace-instrumentation", "").getTracedLogger(getClass),
    )(parallelExecutionContext)

    def close(): Unit = {
      grpc.close()
      telemetry.close()
      TraceContextGrpc.configureGrpcMessageSpanEvents(enabled = false, serverTracer = None)
    }
  }

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new Env()
    try {
      withFixture(test.toNoArgTest(env))
    } finally {
      env.close()
    }
  }

  "gRPC trace instrumentation" should {
    "stay disabled by default" in { env =>
      import env.*

      grpc.server.start()
      val rootSpan = telemetry.tracer.spanBuilder("root").startSpan()
      val traceContext: TraceContext = TraceContext(Context.root().`with`(rootSpan))

      grpc
        .sendRequest()(traceContext, parallelExecutionContext)
        .futureValue shouldBe CantonGrpcUtilTest.response
      rootSpan.end()

      eventually(telemetry.reportedSpans() should have size 1)
      val grpcEvents = eventsOrderedByTime(telemetry.reportedSpans()*)
        .filter(_.getName.startsWith("canton.grpc."))
      grpcEvents shouldBe empty
    }

    "emit unary client and server message events with safe method metadata" in { env =>
      import env.*

      TraceContextGrpc.configureGrpcMessageSpanEvents(
        enabled = true,
        serverTracer = Some(telemetry.tracer),
      )
      grpc.server.start()
      val rootSpan = telemetry.tracer.spanBuilder("root").startSpan()
      val traceContext: TraceContext = TraceContext(Context.root().`with`(rootSpan))

      grpc
        .sendRequest()(traceContext, parallelExecutionContext)
        .futureValue shouldBe CantonGrpcUtilTest.response
      rootSpan.end()

      eventually(telemetry.reportedSpans().map(_.getName) should contain("root"))
      val events = eventsOrderedByTime(telemetry.reportedSpans()*)
      events.map(_.getName) should contain allOf (
        GrpcTraceInstrumentation.ClientMessageSentEventName,
        GrpcTraceInstrumentation.ClientMessageReceivedEventName,
        GrpcTraceInstrumentation.ServerMessageReceivedEventName,
        GrpcTraceInstrumentation.ServerMessageSentEventName,
      )

      val serverReceived = events
        .find(_.getName == GrpcTraceInstrumentation.ServerMessageReceivedEventName)
        .valueOrFail("server receive event")
      serverReceived.getAttributes.get(GrpcTraceInstrumentation.RpcSystemKey) shouldBe "grpc"
      serverReceived.getAttributes.get(GrpcTraceInstrumentation.RpcServiceKey) should include(
        "HelloService"
      )
      serverReceived.getAttributes.get(GrpcTraceInstrumentation.RpcMethodKey) shouldBe "Hello"
      serverReceived.getAttributes.get(GrpcTraceInstrumentation.GrpcDirectionKey) shouldBe "server"
      serverReceived.getAttributes.get(
        GrpcTraceInstrumentation.GrpcMessageDirectionKey
      ) shouldBe "received"
      serverReceived.getAttributes.get(
        GrpcTraceInstrumentation.GrpcCardinalityKey
      ) shouldBe "unary"
      serverReceived.getAttributes.get(
        GrpcTraceInstrumentation.GrpcMessageSequenceKey
      ) shouldBe java.lang.Long.valueOf(1L)
    }

    "emit streaming message sequences on both directions" in { env =>
      import env.*

      TraceContextGrpc.configureGrpcMessageSpanEvents(
        enabled = true,
        serverTracer = Some(telemetry.tracer),
      )
      grpc.server.start()
      val rootSpan = telemetry.tracer.spanBuilder("root").startSpan()
      val traceContext = TraceContext(Context.root().`with`(rootSpan))

      val received = ListBuffer.empty[Hello.Response]
      val completed = new CountDownLatch(1)
      TraceContextGrpc.withGrpcContext(traceContext) {
        grpc.client.service.helloStreamed(
          CantonGrpcUtilTest.request,
          new StreamObserver[Hello.Response] {
            override def onNext(value: Hello.Response): Unit = received.synchronized {
              received += value
            }
            override def onError(t: Throwable): Unit = completed.countDown()
            override def onCompleted(): Unit = completed.countDown()
          },
        )
      }

      completed.await(5, TimeUnit.SECONDS) shouldBe true
      received.toList.map(_.msg) shouldBe List("one", "two")
      rootSpan.end()

      eventually(telemetry.reportedSpans().map(_.getName) should contain("root"))
      val events = eventsOrderedByTime(telemetry.reportedSpans()*)
      val serverSent = events.filter(_.getName == GrpcTraceInstrumentation.ServerMessageSentEventName)
      val clientReceived =
        events.filter(_.getName == GrpcTraceInstrumentation.ClientMessageReceivedEventName)

      serverSent.map(
        _.getAttributes.get(GrpcTraceInstrumentation.GrpcMessageSequenceKey).longValue()
      ) shouldBe List(1L, 2L)
      clientReceived.map(
        _.getAttributes.get(GrpcTraceInstrumentation.GrpcMessageSequenceKey).longValue()
      ) shouldBe List(1L, 2L)
      serverSent.map(
        _.getAttributes.get(GrpcTraceInstrumentation.GrpcCardinalityKey)
      ).distinct shouldBe List("server_streaming")
    }

    "correlate gRPC and executor task events on the same trace" in { _ =>
      val telemetry = new TestTelemetrySetup()
      val observedExecutor = {
        val delegate = Executors.newSingleThreadExecutor(new ThreadFactory {
          private val counter = new java.util.concurrent.atomic.AtomicInteger(0)

          override def newThread(runnable: Runnable): Thread =
            new Thread(runnable, s"grpc-correlated-executor-${counter.incrementAndGet()}")
        })
        new NamedExecutionContextExecutorService(
          delegate,
          "grpc-correlated-executor",
          throwable => noTracingLogger.error("Correlated executor failed", throwable),
          new TraceContextTaskObserver(),
        )
      }
      val grpc = new CantonGrpcUtilTest.Env(
        new HelloService {
          override def hello(request: Hello.Request): Future[Hello.Response] = {
            val task = observedExecutor.submit(new Callable[Hello.Response] {
              override def call(): Hello.Response = CantonGrpcUtilTest.response
            })
            Future(task.get(5, TimeUnit.SECONDS))(parallelExecutionContext)
          }

          override def helloStreamed(
              request: Hello.Request,
              responseObserver: StreamObserver[Hello.Response],
          ): Unit = ()
        },
        loggerFactory.append("grpc-executor-correlation", "").getTracedLogger(getClass),
      )(parallelExecutionContext)

      try {
        TraceContextGrpc.configureGrpcMessageSpanEvents(
          enabled = true,
          serverTracer = Some(telemetry.tracer),
        )
        grpc.server.start()
        val rootSpan = telemetry.tracer.spanBuilder("root").startSpan()
        val traceContext = TraceContext(Context.root().`with`(rootSpan))

        grpc
          .sendRequest()(traceContext, parallelExecutionContext)
          .futureValue shouldBe CantonGrpcUtilTest.response
        rootSpan.end()

        eventually(telemetry.reportedSpans().map(_.getName) should contain("root"))
        val spans = telemetry.reportedSpans()
        val serverSpan = spans
          .find(_.getName.contains("Hello"))
          .valueOrFail("server span")
        serverSpan.getTraceId shouldBe rootSpan.getSpanContext.getTraceId

        val serverEvents = eventsOrderedByTime(serverSpan)
        serverEvents.map(_.getName) should contain(GrpcTraceInstrumentation.ServerMessageReceivedEventName)
        val taskEvent = serverEvents
          .find(_.getName == TraceContextTaskObserver.TaskEventName)
          .valueOrFail("executor task event")
        taskEvent.getAttributes.get(
          TraceContextTaskObserver.ExecutorNameKey
        ) shouldBe "grpc-correlated-executor"
        taskEvent.getAttributes.get(
          TraceContextTaskObserver.WorkerThreadNameKey
        ) should startWith("grpc-correlated-executor-")
      } finally {
        val _ = observedExecutor.shutdownNow()
        grpc.close()
        telemetry.close()
        TraceContextGrpc.configureGrpcMessageSpanEvents(enabled = false, serverTracer = None)
      }
    }

    "attach only whitelisted protocol identifiers from message fields" in { env =>
      final case class NestedCommands(
          commandId: String,
          submissionId: Option[String],
          workflowId: String,
          synchronizerId: String,
          payload: String,
      )
      final case class IdentifiedMessage(
          participantId: String,
          synchronizerId: String,
          requestId: Long,
          submissionId: Option[String],
          commands: Option[NestedCommands],
          payload: String,
      )

      val message = IdentifiedMessage(
        participantId = "participant::id",
        synchronizerId = "synchronizer::id",
        requestId = 7L,
        submissionId = Some("submission-id"),
        commands = Some(
          NestedCommands(
            commandId = "command-id",
            submissionId = Some("nested-submission-id"),
            workflowId = "workflow-id",
            synchronizerId = "nested-synchronizer-id",
            payload = "nested-payload-must-not-be-captured",
          )
        ),
        payload = "payload-must-not-be-captured",
      )
      val attributes = GrpcTraceInstrumentation.protocolIdentifierAttributes(message)

      attributes should contain("canton.protocol.participant_id" -> "participant::id")
      attributes should contain("canton.protocol.synchronizer_id" -> "synchronizer::id")
      attributes should contain("canton.protocol.request_id" -> "7")
      attributes should contain("canton.protocol.submission_id" -> "submission-id")
      attributes should contain("canton.protocol.command_id" -> "command-id")
      attributes should contain("canton.protocol.workflow_id" -> "workflow-id")
      message.payload shouldBe "payload-must-not-be-captured"
      attributes.keys should not contain "payload"

      val spanEventAttributes = GrpcTraceInstrumentation.attributesForMessage(
        env.grpc.helloServiceDefinition.getMethods.iterator().next().getMethodDescriptor,
        GrpcTraceInstrumentation.CallAttributes(direction = "server"),
        "received",
        1L,
        message,
      )
      spanEventAttributes.get(
        stringKey("canton.protocol.participant_id")
      ) shouldBe "participant::id"
      spanEventAttributes.get(
        stringKey("canton.protocol.synchronizer_id")
      ) shouldBe "synchronizer::id"
      spanEventAttributes.get(stringKey("canton.protocol.request_id")) shouldBe "7"
      spanEventAttributes.get(stringKey("canton.protocol.submission_id")) shouldBe "submission-id"
      spanEventAttributes.get(stringKey("canton.protocol.command_id")) shouldBe "command-id"
      spanEventAttributes.get(stringKey("canton.protocol.workflow_id")) shouldBe "workflow-id"
      Option(spanEventAttributes.get(stringKey("payload"))) shouldBe empty
    }
  }
}
