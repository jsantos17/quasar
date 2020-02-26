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

import slamdata.Predef.{Boolean => SBoolean, _}

import quasar.api.{Column, Labeled, QueryEvaluator}
import quasar.api.Label.Syntax._
import quasar.api.push._
import quasar.api.push.param._
import quasar.api.resource.ResourcePath
import quasar.api.table.TableRef
import quasar.connector.destination._
import quasar.connector.render.{ResultRender, RenderConfig}

import cats.data.{Const, EitherT, Ior, NonEmptyList}
import cats.effect.{Concurrent, Timer}
import cats.implicits._

import fs2.Stream

import shims.showToCats

import skolems.∃

final case class PushRunner[
  F[_]: Concurrent: Timer, T, D, Q, R] private (
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[F, Q, Stream[F, R]],
    lookupDestination: D => F[Option[Destination[F]]],
    render: ResultRender[F, R]) {

  def run(
    tableId: T,
    columns: NonEmptyList[Column[SelectedType]],
    destinationId: D,
    path: ResourcePath,
    format: ResultType,
    limit: Option[Long]): F[Either[NonEmptyList[ResultPushError[T, D]], Stream[F, Unit]]] = {

    type Errs = NonEmptyList[ResultPushError[T, D]]

    def err(rpe: ResultPushError[T, D]): Errs = NonEmptyList.one(rpe)

    val writing = for {
      dest <- EitherT.fromOptionF[F, Errs, Destination[F]](
        lookupDestination(destinationId),
        err(ResultPushError.DestinationNotFound(destinationId)))

      tableRef <- EitherT.fromOptionF[F, Errs, TableRef[Q]](
        lookupTable(tableId),
        err(ResultPushError.TableNotFound(tableId)))

      sink <- format match {
        case ResultType.Csv =>
          EitherT.fromOptionF[F, Errs, ResultSink.CreateSink[F, dest.Type]](
            findCsvSink(dest.sinks).pure[F],
            err(ResultPushError.FormatNotSupported(destinationId, format.show)))
      }

      typedColumns <-
        EitherT.fromEither[F](
          columns.traverse(c =>
            c.traverse(constructType(destinationId, dest, c.name, _)).toValidatedNel).toEither)

      evaluated =
        evaluator(tableRef.query)
          .map(_.flatMap(render.render(_, tableRef.columns, sink.config, limit)))

      sinked = sink.consume(path, typedColumns, Stream.force(evaluated))

    } yield sinked

    writing.value
  }

  private def constructType(
    destinationId: D,
    dest: Destination[F],
    name: String,
    selected: SelectedType)
      : Either[ResultPushError[T, D], dest.Type] = {

    import dest._
    import ParamType._

    def checkBounds(b: Int Ior Int, i: Int): SBoolean =
      b.fold(_ <= i, _ >= i, (min, max) => min <= i && i <= max)

    typeIdOrdinal.getOption(selected.index.ordinal) match {
      case Some(id) => construct(id) match {
        case Left(t) => Right(t)

        case Right(e) =>
          val (c, Labeled(label, formal)) = e.value

          val back = for {
            actual <- selected.arg.toRight(ParamError.ParamMissing(label, formal))

            t <- (formal: Formal[A] forSome { type A }, actual) match {
              case (Boolean(_), ∃(Boolean(Const(b)))) =>
                Right(c(b.asInstanceOf[e.A]))

              case (Integer(Integer.Args(None, None)), ∃(Integer(Const(i)))) =>
                Right(c(i.asInstanceOf[e.A]))

              case (Integer(Integer.Args(Some(bounds), None)), ∃(Integer(Const(i)))) =>
                if (checkBounds(bounds, i))
                  Right(c(i.asInstanceOf[e.A]))
                else
                  Left(ParamError.IntOutOfBounds(label, i, bounds))

              case (Integer(Integer.Args(None, Some(step))), ∃(Integer(Const(i)))) =>
                if (step(i))
                  Right(c(i.asInstanceOf[e.A]))
                else
                  Left(ParamError.IntOutOfStep(label, i, step))

              case (Integer(Integer.Args(Some(bounds), Some(step))), ∃(Integer(Const(i)))) =>
                if (!checkBounds(bounds, i))
                  Left(ParamError.IntOutOfBounds(label, i, bounds))
                else if (!step(i))
                  Left(ParamError.IntOutOfStep(label, i, step))
                else
                  Right(c(i.asInstanceOf[e.A]))

              case (Enum(possiblities), ∃(EnumSelect(Const(key)))) =>
                possiblities.lookup(key)
                  .map(a => c(a.asInstanceOf[e.A]))
                  .toRight(ParamError.ValueNotInEnum(label, key, possiblities.keys))

              case _ => Left(ParamError.ParamMismatch(label, formal, actual))
            }
          } yield t

          back.leftMap(err =>
            ResultPushError.TypeConstructionFailed(destinationId, name, id.label, NonEmptyList.one(err)))
      }

      case None =>
        Left(ResultPushError.TypeNotFound(destinationId, name, selected.index))
    }
  }

  private def findCsvSink[T](sinks: NonEmptyList[ResultSink[F, T]]): Option[ResultSink.CreateSink[F, T]] =
    sinks collectFirstSome {
      case csvSink @ ResultSink.CreateSink(RenderConfig.Csv(_, _, _, _, _, _, _, _, _), _) => Some(csvSink)
      case _ => None
    }
}
