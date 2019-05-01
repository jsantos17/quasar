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

import slamdata.Predef.{Boolean, Exception, None, Option, Some, Unit}

import quasar.Condition
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.impl.storage.IndexedStore

import scala.concurrent.duration.FiniteDuration

import cats.effect.Sync

import fs2.Stream

import scalaz.{\/, -\/, \/-, EitherT, Equal, ISet, OptionT}
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

import shims._

final class DefaultDestinations[F[_]: Sync, I: Equal, C: Equal] private (
    freshId: F[I],
    refs: IndexedStore[F, I, DestinationRef[C]],
    errors: DatasourceErrors[F, I])
    extends Destinations[F, Stream[F, ?], I, C] {

  def addDestination(ref: DestinationRef[C]): F[CreateError[C] \/ I] =
    for {
      i <- freshId
      c <- addRef[CreateError[C]](i, ref)
    } yield Condition.disjunctionIso.get(c).as(i)

  def allDestinationMetadata: F[Stream[F, (I, DestinationMeta)]] =
    Sync[F].pure(refs.entries.evalMap {
      case (i, DestinationRef(k, n, _)) =>
        errors.datasourceError(i)
          .map(e => (i, DestinationMeta.fromOption(k, n, e)))
    })

  def destinationRef(destinationId: I): F[ExistentialError[I] \/ DestinationRef[C]] =
    EitherT(lookupRef[ExistentialError[I]](destinationId))
      .map(manager.sanitizedRef)
      .run

  def destinationStatus(destinationId: I): F[ExistentialError[I] \/ Condition[Exception]] =
    EitherT(lookupRef[ExistentialError[I]](destinationId))
      .flatMap(_ => EitherT.rightT(errors.datasourceError(destinationId)))
      .map(Condition.optionIso.reverseGet(_))
      .run

  def removeDestination(destinationId: I): F[Condition[ExistentialError[I]]] =
    refs.delete(destinationId).ifM(
      manager.shutdownDatasource(destinationId).as(Condition.normal[ExistentialError[I]]()),
      Condition.abnormal(datasourceNotFound[I, ExistentialError[I]](destinationId)).point[F])

  def replaceDestination(destinationId: I, ref: DestinationRef[C])
      : F[Condition[DatasourceError[I, C]]] =
    affectsRunning(destinationId, ref) flatMap {
      case \/-(true) => addRef[DatasourceError[I, C]](destinationId, ref)

      case \/-(false) => setRef(destinationId, ref)

      case -\/(err) => Condition.abnormal(err).point[F]
    }

  def supportedDatasourceTypes: F[ISet[DatasourceType]] =
    manager.supportedDatasourceTypes

  ////

  /** Add the ref at the specified id, replacing any running datasource. */
  private def addRef[E >: CreateError[C] <: DatasourceError[I, C]](
      i: I, ref: DestinationRef[C])
      : F[Condition[E]] = {

    type L[X[_], A] = EitherT[X, E, A]
    type M[A] = L[F, A]

    val added = for {
      _ <- EitherT(verifyNameUnique[E](ref.name, i))

      _ <- EitherT(manager.initDatasource(i, ref) map {
        case Condition.Normal() => ().right
        case Condition.Abnormal(e) => (e: E).left
      })

      _ <- refs.insert(i, ref).liftM[L]
    } yield ()

    added.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  /** Returns whether the changes implied by the updated ref
    * affect the running datasource (config or kind changed, etc).
    */
  private def affectsRunning(destinationId: I, updated: DestinationRef[C])
      : F[DatasourceError[I, C] \/ Boolean] =
    EitherT(lookupRef[DatasourceError[I, C]](destinationId)).map {
      case DestinationRef(k, _, c) => (updated.kind =/= k) || (updated.config =/= c)
    }.run

  private def lookupRef[E >: ExistentialError[I] <: DatasourceError[I, C]](
      destinationId: I)
      : F[E \/ DestinationRef[C]] =
    OptionT(refs.lookup(destinationId))
      .toRight(datasourceNotFound[I, E](destinationId))
      .run

  /** Sets the ref for the specified datasource id without affecting the running
    * instance.
    */
  private def setRef(destinationId: I, ref: DestinationRef[C])
      : F[Condition[DatasourceError[I, C]]] = {
    val set =
      for {
        _ <- EitherT(verifyNameUnique[DatasourceError[I, C]](ref.name, destinationId))
        _ <- EitherT.rightT(refs.insert(destinationId, ref))
      } yield ()

    set.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  private def verifyNameUnique[E >: CreateError[C] <: DatasourceError[I, C]](
      name: DestinationName,
      currentId: I)
      : F[E \/ Unit] =
    refs.entries
      .exists(t => t._2.name === name && t._1 =/= currentId)
      .compile.fold(false)(_ || _)
      .map(_ ? datasourceNameExists[E](name).left[Unit] | ().right)
}

object DefaultDestinations {
  def apply[
      F[_]: Sync, I: Equal, C: Equal](
      freshId: F[I],
      refs: IndexedStore[F, I, DestinationRef[C]],
      errors: DatasourceErrors[F, I])
      : Destinations[F, Stream[F, ?], I, C] =
    new DefaultDestinations(freshId, refs, errors)
}
