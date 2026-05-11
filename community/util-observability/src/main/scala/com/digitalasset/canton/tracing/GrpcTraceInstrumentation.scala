// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import io.grpc.{Attributes as GrpcAttributes, CallOptions, MethodDescriptor, ServerCall, Status}
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.AttributeKey.{longKey, stringKey}
import io.opentelemetry.api.common.{Attributes, AttributesBuilder}
import io.opentelemetry.api.trace.{Span, SpanKind, Tracer}

import java.net.InetSocketAddress
import scala.collection.mutable

private[tracing] object GrpcTraceInstrumentation {

  val ClientCallStartedEventName = "canton.grpc.client.call.started"
  val ClientMessageSentEventName = "canton.grpc.client.message.sent"
  val ClientMessageReceivedEventName = "canton.grpc.client.message.received"
  val ClientCallClosedEventName = "canton.grpc.client.call.closed"

  val ServerCallStartedEventName = "canton.grpc.server.call.started"
  val ServerMessageReceivedEventName = "canton.grpc.server.message.received"
  val ServerMessageSentEventName = "canton.grpc.server.message.sent"
  val ServerCallHalfClosedEventName = "canton.grpc.server.call.half_closed"
  val ServerCallClosedEventName = "canton.grpc.server.call.closed"
  val ServerCallCompletedEventName = "canton.grpc.server.call.completed"
  val ServerCallCancelledEventName = "canton.grpc.server.call.cancelled"

  val RpcSystemKey: AttributeKey[String] = stringKey("rpc.system")
  val RpcServiceKey: AttributeKey[String] = stringKey("rpc.service")
  val RpcMethodKey: AttributeKey[String] = stringKey("rpc.method")
  val RpcGrpcFullMethodKey: AttributeKey[String] = stringKey("rpc.grpc.full_method")
  val RpcGrpcStatusCodeKey: AttributeKey[String] = stringKey("rpc.grpc.status_code")
  val GrpcDirectionKey: AttributeKey[String] = stringKey("canton.grpc.direction")
  val GrpcMessageDirectionKey: AttributeKey[String] = stringKey("canton.grpc.message.direction")
  val GrpcMethodTypeKey: AttributeKey[String] = stringKey("canton.grpc.method_type")
  val GrpcCardinalityKey: AttributeKey[String] = stringKey("canton.grpc.cardinality")
  val GrpcMessageSequenceKey: AttributeKey[java.lang.Long] =
    longKey("canton.grpc.message.sequence")
  val GrpcMessageTypeKey: AttributeKey[String] = stringKey("canton.grpc.message.type")
  val GrpcAuthorityKey: AttributeKey[String] = stringKey("canton.grpc.authority")
  val GrpcPeerKey: AttributeKey[String] = stringKey("canton.grpc.peer")
  val ThreadIdKey: AttributeKey[java.lang.Long] = longKey("thread.id")
  val ThreadNameKey: AttributeKey[String] = stringKey("thread.name")

  private val MaxIdentifierLength = 256

  private val IdentifierAttributeNames: Map[String, String] = Map(
    "participantId" -> "canton.protocol.participant_id",
    "participantUid" -> "canton.protocol.participant_uid",
    "synchronizerId" -> "canton.protocol.synchronizer_id",
    "physicalSynchronizerId" -> "canton.protocol.physical_synchronizer_id",
    "synchronizerAlias" -> "canton.protocol.synchronizer_alias",
    "sequencerId" -> "canton.protocol.sequencer_id",
    "mediatorId" -> "canton.protocol.mediator_id",
    "member" -> "canton.protocol.member",
    "memberId" -> "canton.protocol.member_id",
    "requestId" -> "canton.protocol.request_id",
    "submissionId" -> "canton.protocol.submission_id",
    "commandId" -> "canton.protocol.command_id",
    "workflowId" -> "canton.protocol.workflow_id",
    "topologyTransactionId" -> "canton.protocol.topology_transaction_id",
    "topologyMappingId" -> "canton.protocol.topology_mapping_id",
    "transactionId" -> "canton.protocol.transaction_id",
    "updateId" -> "canton.protocol.update_id",
  )

  private val IdentifierKeys: Map[String, AttributeKey[String]] =
    IdentifierAttributeNames.valuesIterator.map(attributeName =>
      attributeName -> stringKey(attributeName)
    ).toMap

  final case class CallAttributes(
      direction: String,
      authority: Option[String] = None,
      peer: Option[String] = None,
      status: Option[Status] = None,
  )

  def startServerSpan[ReqT, RespT](
      tracer: Tracer,
      method: MethodDescriptor[ReqT, RespT],
      parentTraceContext: TraceContext,
      callAttributes: CallAttributes,
  ): (Span, TraceContext) = {
    val span = tracer
      .spanBuilder(method.getFullMethodName)
      .setSpanKind(SpanKind.SERVER)
      .setParent(parentTraceContext.context)
      .startSpan()
    span.setAllAttributes(attributesForCall(method, callAttributes))
    val traceContext = TraceContext(span.storeInContext(parentTraceContext.context))
    (span, traceContext)
  }

  def addEvent(traceContext: TraceContext, name: String, attributes: Attributes): Unit =
    currentSpan(traceContext).foreach(span => {
      val _ = span.addEvent(name, attributes)
    })

  def endSpan(traceContext: TraceContext): Unit =
    currentSpan(traceContext).foreach(_.end())

  def attributesForCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callAttributes: CallAttributes,
  ): Attributes = {
    val builder = baseAttributes(method, callAttributes)
    callAttributes.status.foreach { grpcStatus =>
      builder.put(RpcGrpcStatusCodeKey, grpcStatus.getCode.toString)
      ()
    }
    builder.build()
  }

  def attributesForMessage[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callAttributes: CallAttributes,
      messageDirection: String,
      sequence: Long,
      message: Any,
  ): Attributes = {
    val builder = baseAttributes(method, callAttributes)
      .put(GrpcMessageDirectionKey, messageDirection)
      .put(GrpcMessageSequenceKey, java.lang.Long.valueOf(sequence))
      .put(GrpcMessageTypeKey, Option(message).fold("unknown")(_.getClass.getName))

    addProtocolIdentifierAttributes(builder, message)
    builder.build()
  }

  def callAttributesFromClient(callOptions: CallOptions): CallAttributes =
    CallAttributes(
      direction = "client",
      authority = Option(callOptions.getAuthority),
    )

  def callAttributesFromServer[ReqT, RespT](
      call: ServerCall[ReqT, RespT]
  ): CallAttributes =
    CallAttributes(
      direction = "server",
      authority = Option(call.getAuthority),
      peer = serverPeer(call.getAttributes),
    )

  private def baseAttributes[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callAttributes: CallAttributes,
  ): AttributesBuilder = {
    val thread = Thread.currentThread()
    val builder = Attributes
      .builder()
      .put(RpcSystemKey, "grpc")
      .put(RpcServiceKey, Option(method.getServiceName).getOrElse(""))
      .put(RpcMethodKey, Option(method.getBareMethodName).getOrElse(""))
      .put(RpcGrpcFullMethodKey, method.getFullMethodName)
      .put(GrpcDirectionKey, callAttributes.direction)
      .put(GrpcMethodTypeKey, method.getType.name)
      .put(GrpcCardinalityKey, cardinality(method.getType))
      .put(ThreadIdKey, java.lang.Long.valueOf(thread.getId))
      .put(ThreadNameKey, thread.getName)

    callAttributes.authority.foreach(builder.put(GrpcAuthorityKey, _))
    callAttributes.peer.foreach(builder.put(GrpcPeerKey, _))
    builder
  }

  private def cardinality(methodType: MethodDescriptor.MethodType): String =
    methodType match {
      case MethodDescriptor.MethodType.UNARY => "unary"
      case MethodDescriptor.MethodType.SERVER_STREAMING => "server_streaming"
      case MethodDescriptor.MethodType.CLIENT_STREAMING => "client_streaming"
      case MethodDescriptor.MethodType.BIDI_STREAMING => "bidi_streaming"
      case MethodDescriptor.MethodType.UNKNOWN => "unknown"
    }

  private def currentSpan(traceContext: TraceContext): Option[Span] =
    Option(Span.fromContextOrNull(traceContext.context))
      .filter(_.getSpanContext.isValid)

  private def serverPeer(attributes: GrpcAttributes): Option[String] =
    Option(attributes.get(io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR)).map {
      case inet: InetSocketAddress =>
        val host = Option(inet.getHostString).getOrElse("")
        s"$host:${inet.getPort}"
      case other => other.toString
    }

  private[tracing] def protocolIdentifierAttributes(message: Any): Map[String, String] = {
    val result = mutable.Map.empty[String, String]
    message match {
      case product: Product =>
        product.productElementNames.zip(product.productIterator).foreach {
          case (name, value) =>
            for {
              attributeName <- IdentifierAttributeNames.get(name)
              attributeValue <- safeIdentifierValue(value)
            } result.put(attributeName, attributeValue)
        }
      case _ =>
    }
    result.toMap
  }

  private def addProtocolIdentifierAttributes(builder: AttributesBuilder, message: Any): Unit =
    protocolIdentifierAttributes(message).foreach { case (attributeName, attributeValue) =>
      IdentifierKeys.get(attributeName).foreach(builder.put(_, attributeValue))
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def safeIdentifierValue(value: Any): Option[String] =
    value match {
      case null => None
      case Some(inner) => safeIdentifierValue(inner)
      case None => None
      case string: String if string.nonEmpty => Some(limitIdentifier(string))
      case value: java.lang.Long => Some(value.toString)
      case value: java.lang.Integer => Some(value.toString)
      case value: java.lang.Short => Some(value.toString)
      case value: java.lang.Byte => Some(value.toString)
      case value: java.lang.Boolean => Some(value.toString)
      case value: Long => Some(value.toString)
      case value: Int => Some(value.toString)
      case value: Short => Some(value.toString)
      case value: Byte => Some(value.toString)
      case value: Boolean => Some(value.toString)
      case _ => None
    }

  private def limitIdentifier(value: String): String =
    if (value.length <= MaxIdentifierLength) value else value.take(MaxIdentifierLength)
}
