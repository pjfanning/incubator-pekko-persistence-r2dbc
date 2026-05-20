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

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.util.Try

import pekko.Done
import pekko.actor.ExtendedActorSystem
import pekko.actor.typed.ActorSystem
import pekko.annotation.InternalApi
import pekko.persistence.query.DurableStateChange
import pekko.persistence.r2dbc.session.scaladsl.R2dbcSession
import pekko.persistence.r2dbc.state.javadsl
import pekko.persistence.r2dbc.state.scaladsl.ChangeHandler

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ChangeHandlerFactory {

  /**
   * Adapter from javadsl.ChangeHandler to scaladsl.ChangeHandler
   */
  final class ChangeHandlerAdapter(delegate: javadsl.ChangeHandler[Any]) extends ChangeHandler[Any] {
    override def process(session: R2dbcSession, change: DurableStateChange[Any]): Future[Done] = {
      val javadslSession =
        new akka.persistence.r2dbc.session.javadsl.R2dbcSession(session.connection)(session.ec, session.system)
      delegate.process(javadslSession, change).toScala
    }
  }

  def create(system: ActorSystem[_], fqcn: String): ChangeHandler[Any] = {
    val dynamicAccess = system.classicSystem.asInstanceOf[ExtendedActorSystem].dynamicAccess

    def tryCreateScaladslInstance(): Try[ChangeHandler[Any]] = {
      dynamicAccess
        .createInstanceFor[ChangeHandler[Any]](fqcn, Nil)
        .orElse(
          dynamicAccess
            .createInstanceFor[ChangeHandler[Any]](fqcn, List(classOf[ActorSystem[_]] -> system))
            .orElse(
              dynamicAccess
                .createInstanceFor[ChangeHandler[Any]](
                  fqcn,
                  List(classOf[akka.actor.ActorSystem] -> system.classicSystem))))
    }

    def tryCreateJavadslInstance(): Try[javadsl.ChangeHandler[Any]] = {
      dynamicAccess
        .createInstanceFor[javadsl.ChangeHandler[Any]](fqcn, Nil)
        .orElse(
          dynamicAccess
            .createInstanceFor[javadsl.ChangeHandler[Any]](fqcn, List(classOf[ActorSystem[_]] -> system))
            .orElse(
              dynamicAccess
                .createInstanceFor[javadsl.ChangeHandler[Any]](
                  fqcn,
                  List(classOf[akka.actor.ActorSystem] -> system.classicSystem))))
    }

    def adapt(changeHandler: javadsl.ChangeHandler[Any]): ChangeHandler[Any] =
      new ChangeHandlerAdapter(changeHandler)

    tryCreateScaladslInstance()
      .orElse(tryCreateJavadslInstance().map(adapt))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Additional column [$fqcn] must implement " +
          s"[${classOf[ChangeHandler[_]].getName}] or [${classOf[javadsl.ChangeHandler[_]].getName}]. It " +
          s"may have an ActorSystem constructor parameter."))

  }

}
