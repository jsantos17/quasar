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

import slamdata.Predef._

import quasar.api.destination._
import quasar.api.destination.DestinationError.{
  CreateError,
  ExistentialError
}
import quasar.Condition
import quasar.impl.storage.IndexedStore

import cats.effect.Sync
import fs2.Stream
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._
import scalaz.{\/, Equal, IMap, ISet, OptionT, Order}
import shims._

class DefaultDestinations[F[_]: Sync, I: Equal: Order, C] private (
    freshId: F[I],
    refs: IndexedStore[F, I, DestinationRef[C]],
    manager: DestinationManager[I, C, F])
    extends Destinations[F, Stream[F, ?], I, C] {

  def addDestination(ref: DestinationRef[C]): F[CreateError[C] \/ I] =
    OptionT(refs.entries.filter {
      case (_, r) => r.name === ref.name
    }.compile.last).isDefined.ifM(
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

  def allDestinationMetadata: F[Stream[F, (I, DestinationMeta)]] =
    (refs.entries.evalMap {
      case (i, ref) =>
        manager.errorsOf(i).map(ex => (i, DestinationMeta.fromOption(ref.kind, ref.name, ex)))
    }).point[F]

  def destinationRef(destinationId: I): F[ExistentialError[I] \/ DestinationRef[C]] =
    OptionT(refs.lookup(destinationId))
      .map(manager.sanitizedRef(_))
      .toRight(DestinationError.destinationNotFound(destinationId))
      .run

  def destinationStatus(destinationId: I): F[ExistentialError[I] \/ Condition[Exception]] =
    (refs.lookup(destinationId) |@| manager.errorsOf(destinationId)) {
      case (Some(_), Some(ex)) => Condition.Abnormal(ex).right
      case (Some(_), None) => Condition.normal().right
      case _ =>  DestinationError.destinationNotFound(destinationId).left
    }

  def removeDestination(destinationId: I): F[Condition[ExistentialError[I]]] =
    OptionT(refs.lookup(destinationId)).fold(
      _ => for {
        _ <- refs.delete(destinationId)
        _ <- manager.shutdownDestination(destinationId)
      } yield Condition.normal[ExistentialError[I]](),
      Condition.abnormal(
        DestinationError.destinationNotFound(destinationId)).pure[F]).join

  def replaceDestination(destinationId: I, ref: DestinationRef[C]): F[Condition[DestinationError[I, C]]] =
    refs.lookup(destinationId) >>= {
      case Some(_) => for {
        removeStatus <- removeDestination(destinationId)
        destStatus <- manager.initDestination(destinationId, ref)
        _ <- refs.insert(destinationId, ref)
      } yield destStatus orElse removeStatus
      case None =>
        Condition.abnormal(
          DestinationError.destinationNotFound[I, DestinationError[I, C]](destinationId)).pure[F]
    }

  def supportedDestinationTypes: F[ISet[DestinationType]] =
    manager.supportedDestinationTypes

  def errors: F[IMap[I, Exception]] =
    manager.errors
}

object DefaultDestinations {
  def apply[F[_]: Sync, I: Equal: Order, C](
    freshId: F[I],
    refs: IndexedStore[F, I, DestinationRef[C]],
    manager: DestinationManager[I, C, F]): DefaultDestinations[F, I, C] =
    new DefaultDestinations[F, I, C](freshId, refs, manager)
}
