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

package quasar.impl.datasources

import slamdata.Predef._

import quasar.api.destination._
import quasar.api.destination.DestinationError.CreateError
import quasar.Condition
import quasar.impl.storage.IndexedStore
import quasar.impl.destinations.DestinationManager

import cats.effect.Sync
import fs2.Stream
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._
import scalaz.{\/, Equal, ISet}
import shims._

abstract class DefaultDestinations[F[_]: Sync, I: Equal, C: Equal] private (
    freshId: F[I],
    refs: IndexedStore[F, I, DestinationRef[C]],
    manager: DestinationManager[I, C, F])
    extends Destinations[F, Stream[F, ?], I, C] {

  def addDestination(ref: DestinationRef[C]): F[CreateError[C] \/ I] = {
    val refNameExists = (refs.entries.filter {
      case (_, r) => r.name === ref.name
    }).compile.last.map(_.isDefined)

    refNameExists.ifM(
      DestinationError.destinationNameExists[CreateError[C]](ref.name).left[I].point[F],
      for {
        newId <- freshId
        destStatus <- manager.initDestination(newId, ref)
        result <- destStatus match {
          case Condition.Normal() =>
            refs.insert(newId, ref) *> newId.right[CreateError[C]].point[F]
          case Condition.Abnormal(e) =>
            e.left[I].point[F]
        }
      } yield result)
  }

  def allDestinationMetadata: F[Stream[F, (I, DestinationMeta)]] =
    (refs.entries.map {
      case (i, ref) => (i, DestinationMeta.fromOption(ref.kind, ref.name, None))
    }).point[F]

  def supportedDestinationTypes: F[ISet[DestinationType]] =
    manager.supportedDestinationTypes

}
