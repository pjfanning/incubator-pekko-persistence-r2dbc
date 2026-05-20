/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.pekko.persistence.r2dbc.internal

import pekko.annotation.InternalStableApi
import pekko.persistence.query.UpdatedDurableState
import pekko.persistence.query.typed.EventEnvelope

/**
 * INTERNAL API
 */
@InternalStableApi private[akka] object EnvelopeOrigin {
  val SourceQuery = ""
  val SourceBacktracking = "BT"
  val SourcePubSub = "PS"

  def fromQuery(env: EventEnvelope[_]): Boolean =
    env.source == SourceQuery

  def fromBacktracking(env: EventEnvelope[_]): Boolean =
    env.source == SourceBacktracking

  def fromBacktracking(change: UpdatedDurableState[_]): Boolean =
    change.value == null

  def fromPubSub(env: EventEnvelope[_]): Boolean =
    env.source == SourcePubSub

  def isFilteredEvent(env: Any): Boolean =
    env match {
      case e: EventEnvelope[_] => e.filtered
      case _                   => false
    }

}
