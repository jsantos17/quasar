/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.impl.destinations

import quasar.api.destination.DestinationType
import quasar.connector.{Destination, DestinationModule, MonadResourceErr}

import cats.effect.concurrent.Ref
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import scalaz.syntax.applicative._
import scalaz.{Order, IMap, ISet}
import shims._

abstract class DefaultDestinationManager[I: Order, C, F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer] (
  modules: IMap[DestinationType, DestinationModule],
  running: Ref[F, IMap[I, Destination[F]]]) extends DestinationManager[I, C, F] {

  def supportedDestinationTypes: F[ISet[DestinationType]] =
    modules.keySet.point[F]
}
