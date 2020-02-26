/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.push

import slamdata.Predef._

import quasar.Condition
import quasar.api.{Column, ColumnType, Labeled}
import quasar.api.Label.Syntax._
import quasar.api.push._
import quasar.api.resource.ResourcePath
import quasar.api.table.TableRef
import quasar.connector.destination._

import java.time.Instant
import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.data.{EitherT, OptionT, NonEmptyList}
import cats.effect.{Concurrent, Timer}
import cats.implicits._

import fs2.Stream
import fs2.job.{JobManager, Job, Event => JobEvent}

import skolems.∃

final class DefaultResultPush[
    F[_]: Concurrent: Timer, T, D, Q, R] private (
    lookupTable: T => F[Option[TableRef[Q]]],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, (D, T), Nothing],
    pushRunner: PushRunner[F, T, D, Q, R],
    pushStatus: JMap[D, JMap[T, PushMeta]])
    extends ResultPush[F, T, D] {

  import ResultPushError._

  def coerce(
      destinationId: D,
      tpe: ColumnType.Scalar)
      : F[Either[ResultPushError.DestinationNotFound[D], TypeCoercion[CoercedType]]] = {

    val destF = EitherT.fromOptionF[F, ResultPushError.DestinationNotFound[D], Destination[F]](
      lookupDestination(destinationId),
      ResultPushError.DestinationNotFound(destinationId))

    val coerceF = destF map { dest =>
      import dest._

      dest.coerce(tpe) map { id =>
        val param = construct(id) match {
          case Right(e) =>
            val (_, p) = e.value
            Some(p.map(∃(_)))

          case Left(_) => None
        }

        CoercedType(Labeled(id.label, TypeIndex(typeIdOrdinal(id))), param)
      }
    }

    coerceF.value
  }

  def start(
      tableId: T,
      columns: NonEmptyList[Column[SelectedType]],
      destinationId: D,
      path: ResourcePath,
      format: ResultType,
      limit: Option[Long])
      : F[Condition[NonEmptyList[ResultPushError[T, D]]]] = {

    type Errs = NonEmptyList[ResultPushError[T, D]]

    def err(rpe: ResultPushError[T, D]): Errs = NonEmptyList.one(rpe)

    val sinked = pushRunner.run(tableId, columns, destinationId, path, format, limit)

    val writing = for {
      sinked0 <- EitherT[F, Errs, Stream[F, Unit]](sinked)

      sinked = sinked0.map(Right(_))

      now <- EitherT.right[Errs](instantNow)

      submitted <- EitherT.right[Errs](jobManager.submit(Job((destinationId, tableId), sinked)))

      _ <- EitherT.cond[F](
        submitted,
        (),
        err(ResultPushError.PushAlreadyRunning(tableId, destinationId)))

      pushMeta = PushMeta(path, format, limit, Status.running(now))

      _ <- EitherT.right[Errs](Concurrent[F] delay {
        pushStatus
          .computeIfAbsent(destinationId, _ => new ConcurrentHashMap[T, PushMeta]())
          .put(tableId, pushMeta)
      })
    } yield ()

    writing.value.map(Condition.eitherIso.reverseGet(_))
  }

  def cancel(tableId: T, destinationId: D): F[Condition[ExistentialError[T, D]]] = {
    val doCancel: F[Unit] = for {
      result <- jobManager.cancel((destinationId, tableId))

      now <- instantNow

      _ <- Concurrent[F] delay {
        pushStatus.computeIfPresent(destinationId, (_, mm) => {
          mm.computeIfPresent(tableId, {
            case (_, pm @ PushMeta(_, _, _, Status.Running(startedAt))) =>
              pm.copy(status = Status.canceled(startedAt, now))
            case (_, pm) =>
              pm
          })

          mm
        })
      }
    } yield result

    ensureBothExist[ExistentialError[T, D]](destinationId, tableId)
      .semiflatMap(_ => doCancel)
      .value
      .map(Condition.eitherIso.reverseGet(_))
  }

  def destinationStatus(destinationId: D): F[Either[DestinationNotFound[D], Map[T, PushMeta]]] =
    ensureDestinationExists[DestinationNotFound[D]](destinationId)
      .semiflatMap(x => Concurrent[F] delay {
        val back = Option(pushStatus.get(destinationId))
        back.fold(Map[T, PushMeta]())(_.asScala.toMap)
      })
      .value

  def cancelAll: F[Unit] =
    jobManager.jobIds.flatMap(_.traverse_(jobManager.cancel(_)))


  ////

  private def instantNow: F[Instant] =
    Timer[F].clock.realTime(MILLISECONDS)
      .map(Instant.ofEpochMilli(_))

  private def ensureBothExist[E >: ExistentialError[T, D] <: ResultPushError[T, D]](
      destinationId: D,
      tableId: T)
      : EitherT[F, E, Unit] =
    ensureDestinationExists[E](destinationId) *> ensureTableExists[E](tableId)

  private def ensureDestinationExists[E >: DestinationNotFound[D] <: ResultPushError[T, D]](
      destinationId: D)
      : EitherT[F, E, Unit] =
    OptionT(lookupDestination(destinationId))
      .toRight[E](ResultPushError.DestinationNotFound(destinationId))
      .void

  private def ensureTableExists[E >: TableNotFound[T] <: ResultPushError[T, D]](
      tableId: T)
      : EitherT[F, E, Unit] =
    OptionT(lookupTable(tableId))
      .toRight[E](ResultPushError.TableNotFound(tableId))
      .void
}

object DefaultResultPush {
  def apply[F[_]: Concurrent: Timer, T, D, Q, R](
      lookupTable: T => F[Option[TableRef[Q]]],
      lookupDestination: D => F[Option[Destination[F]]],
      jobManager: JobManager[F, (D, T), Nothing],
      runner: PushRunner[F, T, D, Q, R])
      : F[DefaultResultPush[F, T, D, Q, R]] =
    for {
      pushStatus <- Concurrent[F].delay(new ConcurrentHashMap[D, JMap[T, PushMeta]]())

      // we can't keep track of Completed and Failed jobs in this impl, so we consume them from JobManager
      // and update internal state accordingly
      _ <- Concurrent[F].start((jobManager.events evalMap {
        case JobEvent.Completed((di, ti), start, duration) =>
          Concurrent[F].delay(
            pushStatus.computeIfPresent(di, (_, mm) => {
              mm.computeIfPresent(ti, {
                case (_, pm) =>
                  pm.copy(status = Status.finished(
                    epochToInstant(start.epoch),
                    epochToInstant(start.epoch + duration)))
              })

              mm
            }))

        case JobEvent.Failed((di, ti), start, duration, ex) =>
          Concurrent[F].delay(
            pushStatus.computeIfPresent(di, (_, mm) => {
              mm.computeIfPresent(ti, {
                case (_, pm) =>
                  pm.copy(status = Status.failed(
                    ex,
                    epochToInstant(start.epoch),
                    epochToInstant(start.epoch + duration)))
              })

              mm
            }))
      }).compile.drain)
    } yield new DefaultResultPush(lookupTable, lookupDestination, jobManager, runner, pushStatus)

  private def epochToInstant(e: FiniteDuration): Instant =
    Instant.ofEpochMilli(e.toMillis)
}
