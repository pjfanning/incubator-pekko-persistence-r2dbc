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

import scala.util.Try

import pekko.actor.{ ActorSystem => ClassicActorSystem }
import pekko.actor.ExtendedActorSystem
import pekko.actor.typed.ActorSystem
import pekko.annotation.InternalApi
import pekko.persistence.r2dbc.state.scaladsl.AdditionalColumn
import pekko.persistence.r2dbc.state.javadsl

/**
 * INTERNAL API
 */
@InternalApi private[akka] object AdditionalColumnFactory {

  /**
   * Adapter from javadsl.AdditionColumn to scaladsl.AdditionalColumn
   */
  final class AdditionColumnAdapter(delegate: javadsl.AdditionalColumn[Any, Any]) extends AdditionalColumn[Any, Any] {

    override private[akka] val fieldClass: Class[_] =
      delegate.fieldClass

    override def columnName: String =
      delegate.columnName

    override def bind(upsert: AdditionalColumn.Upsert[Any]): AdditionalColumn.Binding[Any] = {
      val javadslUpsert = new javadsl.AdditionalColumn.Upsert[Any](
        upsert.persistenceId,
        upsert.entityType,
        upsert.slice,
        upsert.revision,
        upsert.value)
      delegate.bind(javadslUpsert) match {
        case bindValue: javadsl.AdditionalColumn.BindValue[_] => AdditionalColumn.BindValue(bindValue.value)
        case javadsl.AdditionalColumn.BindNull                => AdditionalColumn.BindNull
        case javadsl.AdditionalColumn.Skip                    => AdditionalColumn.Skip
      }
    }

  }

  def create(system: ActorSystem[_], fqcn: String): AdditionalColumn[Any, Any] = {
    val dynamicAccess = system.classicSystem.asInstanceOf[ExtendedActorSystem].dynamicAccess

    def tryCreateScaladslInstance(): Try[AdditionalColumn[Any, Any]] = {
      dynamicAccess
        .createInstanceFor[AdditionalColumn[Any, Any]](fqcn, Nil)
        .orElse(
          dynamicAccess
            .createInstanceFor[AdditionalColumn[Any, Any]](fqcn, List(classOf[ActorSystem[_]] -> system))
            .orElse(dynamicAccess.createInstanceFor[AdditionalColumn[Any, Any]](
              fqcn,
              List(classOf[ClassicActorSystem] -> system.classicSystem))))
    }

    def tryCreateJavadslInstance(): Try[javadsl.AdditionalColumn[Any, Any]] = {
      dynamicAccess
        .createInstanceFor[javadsl.AdditionalColumn[Any, Any]](fqcn, Nil)
        .orElse(
          dynamicAccess
            .createInstanceFor[javadsl.AdditionalColumn[Any, Any]](fqcn, List(classOf[ActorSystem[_]] -> system))
            .orElse(dynamicAccess.createInstanceFor[javadsl.AdditionalColumn[Any, Any]](
              fqcn,
              List(classOf[ClassicActorSystem] -> system.classicSystem))))
    }

    def adapt(javadslColumn: javadsl.AdditionalColumn[Any, Any]): AdditionalColumn[Any, Any] =
      new AdditionColumnAdapter(javadslColumn)

    tryCreateScaladslInstance()
      .orElse(tryCreateJavadslInstance().map(adapt))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Additional column [$fqcn] must implement " +
          s"[${classOf[AdditionalColumn[_, _]].getName}] or [${classOf[javadsl.AdditionalColumn[_, _]].getName}]. It " +
          s"may have an ActorSystem constructor parameter."))
  }

}
