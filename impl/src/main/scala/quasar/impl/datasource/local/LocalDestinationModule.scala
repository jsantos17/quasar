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

package quasar.impl.datasource.local

import quasar.api.destination.DestinationError.{
  InitializationError,
  malformedConfiguration
}
import quasar.concurrent.BlockingContext
import quasar.connector.{Destination, DestinationModule, MonadResourceErr}

import argonaut.Json
import cats.effect.{ContextShift, ConcurrentEffect, Resource, Timer}
import scalaz.\/
import scalaz.syntax.applicative._
import shims._

trait LocalDestinationModule extends DestinationModule {
  val blockingPool: BlockingContext

  val destinationType = LocalDestinationType
  def sanitizeDestinationConfig(config: Json): Json = config

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    config: Json)
      : F[InitializationError[Json] \/ Resource[F, Destination[F]]] =
    (for {
      ld <- attemptConfig[F, LocalDestinationConfig, InitializationError[Json]](
        config, "Failed to decode LocalDestination config: ")((c, d) => malformedConfiguration((destinationType, c, d)))
      root <- validatePath(
        ld.rootDir, config, "Invalid destination path: ")((c, d) => malformedConfiguration((destinationType, c, d)))

      dest: Destination[F] = LocalDestination[F](root, blockingPool)
    } yield dest.point[Resource[F, ?]]).run
}
