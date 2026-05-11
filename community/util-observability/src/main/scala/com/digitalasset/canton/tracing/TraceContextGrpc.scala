// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import io.grpc.*
import io.grpc.Context as GrpcContext
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.opentelemetry.api.trace.{Span, Tracer}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.util.{Try, Using}

/** Support for propagating TraceContext values across GRPC boundaries. Includes:
  *   - a client interceptor for setting context values when sending requests to a server
  *   - a server interceptor for receiving context values when receiving requests from a client
  */
object TraceContextGrpc {
  // value of trace context in the GRPC Context
  private val TraceContextKey =
    Context.keyWithDefault[TraceContext]("traceContext", TraceContext.empty)

  val TraceContextOptionsKey = CallOptions.Key.create[TraceContext]("traceContext")

  private val GrpcMessageSpanEventsEnabled = new AtomicBoolean(false)
  private val GrpcMessageSpanEventsServerTracer = new AtomicReference[Option[Tracer]](None)

  def configureGrpcMessageSpanEvents(enabled: Boolean, serverTracer: Option[Tracer]): Unit = {
    GrpcMessageSpanEventsEnabled.set(enabled)
    GrpcMessageSpanEventsServerTracer.set(serverTracer.filter(_ => enabled))
  }

  def grpcMessageSpanEventsEnabled: Boolean = GrpcMessageSpanEventsEnabled.get()

  def fromGrpcContext: TraceContext = TraceContextKey.get()

  def fromGrpcContextOrNew(name: String): TraceContext = {
    val grpcTraceContext = TraceContextGrpc.fromGrpcContext
    if (grpcTraceContext.traceId.isDefined) {
      grpcTraceContext
    } else {
      TraceContext.withNewTraceContext(name)(identity)
    }
  }

  def withGrpcTraceContext[A](f: TraceContext => A): A = f(fromGrpcContext)

  def withGrpcContext[A](traceContext: TraceContext)(fn: => A): A = {
    val context = GrpcContext.current().withValue(TraceContextKey, traceContext)

    context.call(() => fn)
  }

  private implicit final class TryFailedOps[A](private val a: Try[A]) extends AnyVal {
    @inline
    def valueOrThrow: A = a.fold(throw _, identity)
  }

  /** Injects headers TraceContext from into headers
    * @param wrappedInterceptor
    *   an optional interceptor to wrap with traceContext.context.makeCurrent(), to allow
    *   interoperability with GrpcTelemetry or other context-propagating interceptors
    */
  def clientInterceptor(wrappedInterceptor: Option[ClientInterceptor] = None): ClientInterceptor =
    new TraceContextClientInterceptor(wrappedInterceptor)
  def serverInterceptor: ServerInterceptor = new TraceContextServerInterceptor

  def reportingServerInterceptor(tracer: Tracer): ServerInterceptor =
    new TraceContextReportingServerInterceptor(tracer)

  private class TraceContextClientInterceptor(wrappedInterceptor: Option[ClientInterceptor])
      extends ClientInterceptor {
    override def interceptCall[ReqT, RespT](
        method: MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions,
        next: Channel,
    ): ClientCall[ReqT, RespT] = {
      val tcOpts = Option(callOptions.getOption(TraceContextOptionsKey))
      val traceContext = tcOpts.getOrElse(TraceContextKey.get())
      val contextToPropagate = traceContext.context

      def withPropagatedContext[T](fn: => T): T =
        wrappedInterceptor match {
          case Some(_) =>
            Using(contextToPropagate.makeCurrent()) { _ =>
              fn
            }.valueOrThrow
          case None => fn
        }

      val nextCall = withPropagatedContext {
        wrappedInterceptor.fold(next.newCall(method, callOptions))(
          _.interceptCall(method, callOptions, next)
        )
      }

      new SimpleForwardingClientCall[ReqT, RespT](nextCall) {
        private val sentSequence = new AtomicLong(0)
        private val receivedSequence = new AtomicLong(0)
        private val callAttributes =
          GrpcTraceInstrumentation.callAttributesFromClient(callOptions)
        private val emitMessageEvents = grpcMessageSpanEventsEnabled

        override def start(
            responseListener: ClientCall.Listener[RespT],
            headers: Metadata,
        ): Unit = {

          W3CTraceContext.injectIntoGrpcMetadata(traceContext, headers)

          if (emitMessageEvents) {
            GrpcTraceInstrumentation.addEvent(
              traceContext,
              GrpcTraceInstrumentation.ClientCallStartedEventName,
              GrpcTraceInstrumentation.attributesForCall(method, callAttributes),
            )
          }

          val listener =
            if (emitMessageEvents)
              new InstrumentedClientCallListener(
                responseListener,
                method,
                traceContext,
                callAttributes,
                receivedSequence,
              )
            else responseListener

          super.start(listener, headers)
        }

        override def sendMessage(message: ReqT): Unit = {
          if (emitMessageEvents) {
            GrpcTraceInstrumentation.addEvent(
              traceContext,
              GrpcTraceInstrumentation.ClientMessageSentEventName,
              GrpcTraceInstrumentation.attributesForMessage(
                method,
                callAttributes,
                "sent",
                sentSequence.incrementAndGet(),
                message,
              ),
            )
          }
          super.sendMessage(message)
        }
      }
    }
  }

  private class TraceContextServerInterceptor extends ServerInterceptor {
    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT],
    ): ServerCall.Listener[ReqT] = {
      val traceContextFromWire = W3CTraceContext.fromGrpcMetadata(headers)
      val callAttributes = GrpcTraceInstrumentation.callAttributesFromServer(call)
      val (traceContext, endServerSpan) =
        if (grpcMessageSpanEventsEnabled) {
          GrpcMessageSpanEventsServerTracer.get() match {
            case Some(tracer) =>
              val (span, serverTraceContext) = GrpcTraceInstrumentation.startServerSpan(
                tracer,
                call.getMethodDescriptor,
                traceContextFromWire,
                callAttributes,
              )
              val _ = span.addEvent(
                GrpcTraceInstrumentation.ServerCallStartedEventName,
                GrpcTraceInstrumentation.attributesForCall(call.getMethodDescriptor, callAttributes),
              )
              (serverTraceContext, true)
            case None =>
              GrpcTraceInstrumentation.addEvent(
                traceContextFromWire,
                GrpcTraceInstrumentation.ServerCallStartedEventName,
                GrpcTraceInstrumentation.attributesForCall(call.getMethodDescriptor, callAttributes),
              )
              (traceContextFromWire, false)
          }
        } else (traceContextFromWire, false)

      val context = GrpcContext
        .current()
        .withValue(TraceContextKey, traceContext)

      val serverCall =
        if (grpcMessageSpanEventsEnabled)
          new InstrumentedServerCall(call, traceContext, callAttributes)
        else call
      val listener = Contexts.interceptCall(context, serverCall, headers, next)
      if (grpcMessageSpanEventsEnabled)
        new InstrumentedServerCallListener(
          listener,
          call.getMethodDescriptor,
          traceContext,
          callAttributes,
          endServerSpan,
        )
      else listener
    }
  }

  private class TraceContextReportingServerInterceptor(tracer: Tracer) extends ServerInterceptor {
    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT],
    ): ServerCall.Listener[ReqT] = {

      val traceContextFromWire = W3CTraceContext.fromGrpcMetadata(headers)
      val parentTraceContext =
        if (traceContextFromWire.traceId.isEmpty)
          TraceContext.withNewTraceContext("grpc-server")(identity)
        else
          traceContextFromWire

      val (span, traceContext, callAttributesO) =
        if (grpcMessageSpanEventsEnabled) {
          val callAttributes = GrpcTraceInstrumentation.callAttributesFromServer(call)
          val (serverSpan, serverTraceContext) =
            GrpcTraceInstrumentation.startServerSpan(
              tracer,
              call.getMethodDescriptor,
              parentTraceContext,
              callAttributes,
            )
          val _ = serverSpan.addEvent(
            GrpcTraceInstrumentation.ServerCallStartedEventName,
            GrpcTraceInstrumentation.attributesForCall(call.getMethodDescriptor, callAttributes),
          )
          (serverSpan, serverTraceContext, Some(callAttributes))
        } else {
          val serverSpan = tracer
            .spanBuilder(call.getMethodDescriptor.getFullMethodName)
            .setParent(parentTraceContext.context)
            .startSpan()
          val serverTraceContext = TraceContext(
            serverSpan.storeInContext(parentTraceContext.context)
          )
          (serverSpan, serverTraceContext, None)
        }

      val context = GrpcContext
        .current()
        .withValue(TraceContextKey, traceContext)

      val serverCall =
        callAttributesO.fold(call)(callAttributes =>
          new InstrumentedServerCall(call, traceContext, callAttributes)
        )
      val nextServerCallListener = Contexts.interceptCall(context, serverCall, headers, next)
      callAttributesO match {
        case Some(callAttributes) =>
          new InstrumentedServerCallListener(
            nextServerCallListener,
            call.getMethodDescriptor,
            traceContext,
            callAttributes,
            endServerSpan = true,
          )
        case None => new ServerCallListener(nextServerCallListener, span)
      }
    }

    /** Intercepts events sent by the client.
      */
    class ServerCallListener[ReqT, RespT](
        delegate: ServerCall.Listener[ReqT],
        span: Span,
    ) extends SimpleForwardingServerCallListener[ReqT](delegate) {

      /** Called when the server receives the request. */
      override def onMessage(message: ReqT): Unit =
        delegate.onMessage(message)

      /** Called when the client completed all message sending (except for cancellation). */
      override def onHalfClose(): Unit =
        delegate.onHalfClose()

      /** Called when the client cancels the call. */
      override def onCancel(): Unit = {
        delegate.onCancel()
        span.end()
      }

      /** Called when the server considers the call completed. */
      override def onComplete(): Unit = {
        delegate.onComplete()
        span.end()
      }

      override def onReady(): Unit =
        delegate.onReady()
    }
  }

  private class InstrumentedClientCallListener[RespT](
      delegate: ClientCall.Listener[RespT],
      method: MethodDescriptor[?, RespT],
      traceContext: TraceContext,
      callAttributes: GrpcTraceInstrumentation.CallAttributes,
      receivedSequence: AtomicLong,
  ) extends SimpleForwardingClientCallListener[RespT](delegate) {

    override def onMessage(message: RespT): Unit = {
      GrpcTraceInstrumentation.addEvent(
        traceContext,
        GrpcTraceInstrumentation.ClientMessageReceivedEventName,
        GrpcTraceInstrumentation.attributesForMessage(
          method,
          callAttributes,
          "received",
          receivedSequence.incrementAndGet(),
          message,
        ),
      )
      delegate.onMessage(message)
    }

    override def onClose(status: Status, trailers: Metadata): Unit = {
      GrpcTraceInstrumentation.addEvent(
        traceContext,
        GrpcTraceInstrumentation.ClientCallClosedEventName,
        GrpcTraceInstrumentation.attributesForCall(
          method,
          callAttributes.copy(status = Some(status)),
        ),
      )
      delegate.onClose(status, trailers)
    }
  }

  private class InstrumentedServerCall[ReqT, RespT](
      delegate: ServerCall[ReqT, RespT],
      traceContext: TraceContext,
      callAttributes: GrpcTraceInstrumentation.CallAttributes,
  ) extends SimpleForwardingServerCall[ReqT, RespT](delegate) {

    private val sentSequence = new AtomicLong(0)
    private val method = delegate.getMethodDescriptor

    override def sendMessage(message: RespT): Unit = {
      GrpcTraceInstrumentation.addEvent(
        traceContext,
        GrpcTraceInstrumentation.ServerMessageSentEventName,
        GrpcTraceInstrumentation.attributesForMessage(
          method,
          callAttributes,
          "sent",
          sentSequence.incrementAndGet(),
          message,
        ),
      )
      delegate.sendMessage(message)
    }

    override def close(status: Status, trailers: Metadata): Unit = {
      GrpcTraceInstrumentation.addEvent(
        traceContext,
        GrpcTraceInstrumentation.ServerCallClosedEventName,
        GrpcTraceInstrumentation.attributesForCall(
          method,
          callAttributes.copy(status = Some(status)),
        ),
      )
      delegate.close(status, trailers)
    }
  }

  private class InstrumentedServerCallListener[ReqT, RespT](
      delegate: ServerCall.Listener[ReqT],
      method: MethodDescriptor[ReqT, RespT],
      traceContext: TraceContext,
      callAttributes: GrpcTraceInstrumentation.CallAttributes,
      endServerSpan: Boolean,
  ) extends SimpleForwardingServerCallListener[ReqT](delegate) {

    private val receivedSequence = new AtomicLong(0)

    override def onMessage(message: ReqT): Unit = {
      GrpcTraceInstrumentation.addEvent(
        traceContext,
        GrpcTraceInstrumentation.ServerMessageReceivedEventName,
        GrpcTraceInstrumentation.attributesForMessage(
          method,
          callAttributes,
          "received",
          receivedSequence.incrementAndGet(),
          message,
        ),
      )
      delegate.onMessage(message)
    }

    override def onHalfClose(): Unit = {
      GrpcTraceInstrumentation.addEvent(
        traceContext,
        GrpcTraceInstrumentation.ServerCallHalfClosedEventName,
        GrpcTraceInstrumentation.attributesForCall(method, callAttributes),
      )
      delegate.onHalfClose()
    }

    override def onCancel(): Unit = {
      GrpcTraceInstrumentation.addEvent(
        traceContext,
        GrpcTraceInstrumentation.ServerCallCancelledEventName,
        GrpcTraceInstrumentation.attributesForCall(method, callAttributes),
      )
      try {
        delegate.onCancel()
      } finally {
        if (endServerSpan) GrpcTraceInstrumentation.endSpan(traceContext)
      }
    }

    override def onComplete(): Unit = {
      GrpcTraceInstrumentation.addEvent(
        traceContext,
        GrpcTraceInstrumentation.ServerCallCompletedEventName,
        GrpcTraceInstrumentation.attributesForCall(method, callAttributes),
      )
      try {
        delegate.onComplete()
      } finally {
        if (endServerSpan) GrpcTraceInstrumentation.endSpan(traceContext)
      }
    }

    override def onReady(): Unit =
      delegate.onReady()
  }
}
