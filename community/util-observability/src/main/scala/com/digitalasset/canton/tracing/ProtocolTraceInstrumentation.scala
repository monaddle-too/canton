// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.{Attributes, AttributesBuilder}
import io.opentelemetry.api.trace.Span

import scala.collection.mutable

/** Payload-safe protocol identifier enrichment for high-fidelity local observability.
  *
  * The helper only records whitelisted scalar identifiers. Nested extraction is bounded and follows
  * named protobuf envelope fields such as `commands`, `completion`, and `update`; it never records
  * arbitrary request/response payload fields.
  */
object ProtocolTraceInstrumentation {

  val IdentifierEventName = "canton.protocol.identifiers"

  val BoundaryKey: AttributeKey[String] = stringKey("canton.protocol.boundary")

  private val MaxIdentifierLength = 256
  private val MaxNestedIdentifierDepth = 4

  private[tracing] val IdentifierAttributeNames: Map[String, String] = Map(
    "participantId" -> "canton.protocol.participant_id",
    "participantUid" -> "canton.protocol.participant_uid",
    "synchronizerId" -> "canton.protocol.synchronizer_id",
    "sourceSynchronizerId" -> "canton.protocol.source_synchronizer_id",
    "targetSynchronizerId" -> "canton.protocol.target_synchronizer_id",
    "expectedSynchronizerId" -> "canton.protocol.expected_synchronizer_id",
    "physicalSynchronizerId" -> "canton.protocol.physical_synchronizer_id",
    "sourcePhysicalSynchronizerId" -> "canton.protocol.source_physical_synchronizer_id",
    "targetPhysicalSynchronizerId" -> "canton.protocol.target_physical_synchronizer_id",
    "successorPhysicalSynchronizerId" -> "canton.protocol.successor_physical_synchronizer_id",
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

  private val EnvelopeFieldNames: Set[String] = Set(
    "commands",
    "reassignmentCommands",
    "completion",
    "request",
    "response",
    "result",
    "update",
    "transaction",
    "reassignment",
    "topologyTransaction",
    "topologyTransactions",
    "topologyMapping",
    "value",
  )

  def emitIdentifierEvent(
      boundary: String,
      identifiers: (String, Any)*,
  )(implicit traceContext: TraceContext): Unit =
    if (TraceContextGrpc.grpcMessageSpanEventsEnabled) {
      currentSpan(traceContext).foreach { span =>
        val _ = span.addEvent(IdentifierEventName, attributesForIdentifiers(boundary, identifiers))
      }
    }

  def attributesForIdentifiers(
      boundary: String,
      identifiers: Iterable[(String, Any)],
  ): Attributes = {
    val builder = Attributes.builder().put(BoundaryKey, boundary)
    addIdentifierAttributes(builder, identifiers)
    builder.build()
  }

  private[tracing] def protocolIdentifierAttributes(message: Any): Map[String, String] =
    collectProtocolIdentifierAttributes(message, MaxNestedIdentifierDepth)

  private[tracing] def addProtocolIdentifierAttributes(
      builder: AttributesBuilder,
      message: Any,
  ): Unit =
    addIdentifierAttributes(builder, protocolIdentifierAttributes(message))

  private def addIdentifierAttributes(
      builder: AttributesBuilder,
      identifiers: Iterable[(String, Any)],
  ): Unit =
    identifiers.foreach { case (name, value) =>
      val attributeName = IdentifierAttributeNames.getOrElse(name, name)
      for {
        key <- IdentifierKeys.get(attributeName)
        safeValue <- safeIdentifierValue(value)
      } builder.put(key, safeValue)
    }

  private def collectProtocolIdentifierAttributes(
      value: Any,
      remainingDepth: Int,
  ): Map[String, String] = {
    val result = mutable.LinkedHashMap.empty[String, String]

    def addAll(attributes: Map[String, String]): Unit =
      attributes.foreach { case (name, value) =>
        val _ = result.getOrElseUpdate(name, value)
      }

    value match {
      case Some(inner) =>
        addAll(collectProtocolIdentifierAttributes(inner, remainingDepth))
      case product: Product =>
        product.productElementNames.zip(product.productIterator).foreach {
          case (name, fieldValue) =>
            for {
              attributeName <- IdentifierAttributeNames.get(name)
              attributeValue <- safeIdentifierValue(fieldValue)
            } {
              val _ = result.getOrElseUpdate(attributeName, attributeValue)
            }

            if (remainingDepth > 0 && EnvelopeFieldNames.contains(name))
              addAll(collectProtocolIdentifierAttributes(fieldValue, remainingDepth - 1))
        }
      case _ =>
    }

    result.toMap
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

  private def currentSpan(traceContext: TraceContext): Option[Span] =
    Option(Span.fromContextOrNull(traceContext.context))
      .filter(_.getSpanContext.isValid)

  private def limitIdentifier(value: String): String =
    if (value.length <= MaxIdentifierLength) value else value.take(MaxIdentifierLength)
}
