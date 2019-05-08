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

import quasar.{Condition, Disposable, RenderTreeT}
import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError.{CreateError, DatasourceUnsupported}
import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, QueryResult}
import quasar.contrib.scalaz._
import quasar.fp.ski.κ2
import quasar.impl.DatasourceModule
import quasar.impl.datasource.{ByNeedDatasource, FailedDatasource}
import quasar.impl.IncompatibleModuleException.linkDatasource
import quasar.qscript.{InterpretedRead, MonadPlannerErr}

import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.Argonaut.jEmptyObject

import cats.ApplicativeError
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.syntax.applicativeError._

import fs2.Stream

import matryoshka.{BirecursiveT, EqualT, ShowT}

import scalaz.{EitherT, IMap, ISet, OptionT, Order, Scalaz}, Scalaz._

import shims._

final class DefaultDatasourceManager[
    I: Order,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: Timer,
    G[_],
    R] private (
    modules: DefaultDatasourceManager.Modules,
    running: Ref[F, DefaultDatasourceManager.Running[I, T, F, G, R]],
    onCreate: (I, DefaultDatasourceManager.MDS[T, F]) => F[ManagedDatasource[T, F, G, R]])(
    implicit
    ec: ExecutionContext)
    extends DatasourceManager[I, Json, T, F, G, R] { self =>

  import DefaultDatasourceManager._

  type MDS = DefaultDatasourceManager.MDS[T, F]
  type Running = DefaultDatasourceManager.Running[I, T, F, G, R]

  def initDatasource(datasourceId: I, ref: DatasourceRef[Json])
      : F[Condition[CreateError[Json]]] = {

    def createDatasource(module: DatasourceModule)
        : EitherT[F, CreateError[Json], Disposable[F, MDS]] =
      module match {
        case DatasourceModule.Lightweight(lw) =>
          EitherT(handleLinkageError(module.kind, lw.lightweightDatasource[F](ref.config)))
            .bimap(ie => ie: CreateError[Json], _.map(ManagedDatasource.lightweight[T](_)))

        case DatasourceModule.Heavyweight(hw) =>
          EitherT(handleLinkageError(module.kind, hw.heavyweightDatasource[T, F](ref.config)))
            .bimap(ie => ie: CreateError[Json], _.map(ManagedDatasource.heavyweight(_)))
      }

    val inited = for {
      sup <- EitherT.rightU[CreateError[Json]](supportedDatasourceTypes)

      mod <-
        OptionT(modules.lookup(ref.kind).point[F])
          .toRight[CreateError[Json]](DatasourceUnsupported(ref.kind, sup))

      mds <- createDatasource(mod)

      ds <- EitherT.rightT(onCreate(datasourceId, mds.unsafeValue) onError {
        case _ => mds.dispose
      })

      dds = Disposable(ds, mds.dispose)

      _ <- EitherT.rightT(modifyAndShutdown { r =>
        (r.insert(datasourceId, dds), r.lookup(datasourceId))
      })
    } yield ()

    inited.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  def managedDatasource(datasourceId: I): F[Option[ManagedDatasource[T, F, G, R]]] =
    running.get.map(_.lookup(datasourceId).map(_.unsafeValue))

  def sanitizedRef(ref: DatasourceRef[Json]): DatasourceRef[Json] =
    modules.lookup(ref.kind)
      .fold(configL.set(jEmptyObject))(m => configL.modify(m.sanitizeConfig))
      .apply(ref)

  def shutdownDatasource(datasourceId: I): F[Unit] =
    modifyAndShutdown(_.updateLookupWithKey(datasourceId, κ2(None)).swap)

  val supportedDatasourceTypes: F[ISet[DatasourceType]] =
    modules.keySet.point[F]

  ////

  private val configL = DatasourceRef.config[Json]

  private def modifyAndShutdown(
      f: Running => (Running, Option[Disposable[F, ManagedDatasource[T, F, G, R]]]))
      : F[Unit] =
    running.modify(f).flatMap(_.traverse_(_.dispose))
}

object DefaultDatasourceManager {
  type Modules = IMap[DatasourceType, DatasourceModule]
  type MDS[T[_[_]], F[_]] = ManagedDatasource[T, F, Stream[F, ?], QueryResult[F]]
  type Running[I, T[_[_]], F[_], G[_], R] = IMap[I, Disposable[F, ManagedDatasource[T, F, G, R]]]
  type MonadCreateErr[F[_]] = MonadError_[F, CreateError[Json]]

  final class Builder[
      I: Order,
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadCreateErr: Timer,
      G[_],
      R] private (
      onCreate: (I, MDS[T, F]) => F[ManagedDatasource[T, F, G, R]]) {

    def build(modules: Modules, configured: IMap[I, DatasourceRef[Json]])(
        implicit
        ec: ExecutionContext)
        : Resource[F, DatasourceManager[I, Json, T, F, G, R]] = {

      val init = for {
        bases <- configured traverse { ref =>
          modules.lookup(ref.kind) match {
            case Some(mod) =>
              lazyDatasource[T, F](mod, ref)

            case None =>
              val ds = FailedDatasource[CreateError[Json], F, Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F]](
                ref.kind,
                DatasourceUnsupported(ref.kind, modules.keySet))

              ManagedDatasource.lightweight[T](ds).point[Disposable[F, ?]].point[F]
          }
        }

        mapped <- bases.toList traverse {
          case (id, mds) =>
            onCreate(id, mds.unsafeValue)
              .map(ds => (id, mds.as(ds)))
              .onError { case _ => mds.dispose }
        }

        running <- Ref[F].of(IMap.fromList(mapped))

        runningKeys = running.get.map(_.keySet)

      } yield (new DefaultDatasourceManager(modules, running, onCreate), runningKeys)

      val resource = Resource.make(init) {
        case (mgr, keys) => keys flatMap { ks =>
          ks.foldMapLeft1Opt(mgr.shutdownDatasource)((u, k) => u.guarantee(mgr.shutdownDatasource(k)))
            .sequence_
        }
      }

      resource.map(_._1)
    }

    def withMiddleware[H[_], S](
        f: (I, ManagedDatasource[T, F, G, R]) => F[ManagedDatasource[T, F, H, S]])
        : Builder[I, T, F, H, S] =
      new Builder[I, T, F, H, S]((i, mds) => onCreate(i, mds).flatMap(f(i, _)))
  }

  object Builder {
    def apply[
        I: Order,
        T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
        F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadCreateErr: Timer]
        : Builder[I, T, F, Stream[F, ?], QueryResult[F]] =
      new Builder[I, T, F, Stream[F, ?], QueryResult[F]]((_, mds) => mds.point[F])
  }

  ////

  private def lazyDatasource[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadCreateErr: Timer](
      module: DatasourceModule,
      ref: DatasourceRef[Json])(
      implicit ec: ExecutionContext)
      : F[Disposable[F, MDS[T, F]]] =
    module match {
      case DatasourceModule.Lightweight(lw) =>
        val mklw = MonadError_[F, CreateError[Json]] unattempt {
          handleLinkageError(ref.kind, lw.lightweightDatasource[F](ref.config))
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDatasource(ref.kind, mklw)
          .map(_.map(ManagedDatasource.lightweight[T](_)))

      case DatasourceModule.Heavyweight(hw) =>
        val mkhw = MonadError_[F, CreateError[Json]] unattempt {
          handleLinkageError(ref.kind, hw.heavyweightDatasource[T, F](ref.config))
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDatasource(ref.kind, mkhw)
          .map(_.map(ManagedDatasource.heavyweight(_)))
    }

  private def handleLinkageError[F[_]: ApplicativeError[?[_], Throwable], A](kind: DatasourceType, fa: => F[A]): F[A] =
    linkDatasource(kind, fa)
}
