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

package quasar.run

import slamdata.Predef._

import quasar.RateLimiting
import quasar.api.{QueryEvaluator, SchemaConfig}
import quasar.api.datasource.{DatasourceRef, DatasourceType, Datasources}
import quasar.api.destination.{DestinationRef, DestinationType, Destinations}
import quasar.api.push.ResultPush
import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.api.table.{TableRef, Tables}
import quasar.common.PhaseResultTell
import quasar.connector.{QueryResult, ResourceSchema}
import quasar.connector.datasource.Datasource
import quasar.connector.destination.{Destination, DestinationModule}
import quasar.connector.evaluate._
import quasar.connector.render.ResultRender
import quasar.contrib.std.uuid._
import quasar.ejson.implicits._
import quasar.impl.{DatasourceModule, QuasarDatasource, ResourceManager, UuidString}
import quasar.impl.datasource.{AggregateResult, CompositeResult}
import quasar.impl.datasources._
import quasar.impl.datasources.middleware._
import quasar.impl.destinations._
import quasar.impl.evaluate._
import quasar.impl.push.{DefaultResultPush, PushRunner}
import quasar.impl.storage.IndexedStore
import quasar.impl.table.DefaultTables
import quasar.qscript.{construction, Map => QSMap}
import quasar.run.implicits._

import java.util.UUID
import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.JsonScalaz._

import cats.{~>, Functor, Show}
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.kernel.Hash
import cats.syntax.functor._
import cats.syntax.show._

import fs2.Stream
import fs2.job.JobManager

import matryoshka.data.Fix

import org.slf4s.Logging

import shims.{monadToScalaz, functorToCats, functorToScalaz, orderToScalaz, showToCats}

final class Quasar[F[_], R, C <: SchemaConfig](
    val datasources: Datasources[F, Stream[F, ?], UUID, Json, C],
    val destinations: Destinations[F, Stream[F, ?], UUID, Json],
    val tables: Tables[F, UUID, SqlQuery],
    val queryEvaluator: QueryEvaluator[F, SqlQuery, Stream[F, R]],
    val resultPush: ResultPush[F, UUID, UUID])

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
object Quasar extends Logging {

  type EvalResult[F[_]] = Either[QueryResult[F], AggregateResult[F, QSMap[Fix, QueryResult[F]]]]

  /** What it says on the tin. */
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultTell: Timer, R, C <: SchemaConfig, A: Hash](
      datasourceRefs: IndexedStore[F, UUID, DatasourceRef[Json]],
      destinationRefs: IndexedStore[F, UUID, DestinationRef[Json]],
      tableRefs: IndexedStore[F, UUID, TableRef[SqlQuery]],
      queryFederation: QueryFederation[Fix, F, QueryAssociate[Fix, F, EvalResult[F]], Stream[F, R]],
      resultRender: ResultRender[F, R],
      resourceSchema: ResourceSchema[F, C, (ResourcePath, CompositeResult[F, QueryResult[F]])],
      rateLimiting: RateLimiting[F, A],
      byteStores: ByteStores[F, UUID])(
      datasourceModules: List[DatasourceModule],
      destinationModules: List[DestinationModule])(
      implicit
      ec: ExecutionContext)
      : Resource[F, Quasar[F, R, C]] = {

    val destModules =
      DestinationModules[F, UUID](destinationModules)

    for {
      _ <- Resource.liftF(warnDuplicates[F, DatasourceModule, DatasourceType](datasourceModules)(_.kind))
      _ <- Resource.liftF(warnDuplicates[F, DestinationModule, DestinationType](destinationModules)(_.destinationType))

      freshUUID = Sync[F].delay(UUID.randomUUID)

      (dsErrors, onCondition) <- Resource.liftF(DefaultDatasourceErrors[F, UUID])

      dsModules =
        DatasourceModules[Fix, F, UUID, A](datasourceModules, rateLimiting, byteStores)
          .withMiddleware(AggregatingMiddleware(_, _))
          .withMiddleware(ConditionReportingMiddleware(onCondition)(_, _))

      dsCache <- ResourceManager[F, UUID, QuasarDatasource[Fix, F, Stream[F, ?], CompositeResult[F, QueryResult[F]], ResourcePathType]]
      datasources <- Resource.liftF(DefaultDatasources(freshUUID, datasourceRefs, dsModules, dsCache, dsErrors, resourceSchema, byteStores))

      destCache <- ResourceManager[F, UUID, Destination[F]]
      destinations <- Resource.liftF(DefaultDestinations(freshUUID, destinationRefs, destCache, destModules))

      lookupRunning =
        (id: UUID) => datasources.quasarDatasourceOf(id).map(_.map(_.modify(reifiedAggregateDs)))

      sqlEvaluator =
        Sql2Compiler[Fix, F]
          .map((_, None))
          .andThen(QueryFederator(ResourceRouter(UuidString, lookupRunning)))
          .andThen(queryFederation)

      tables = DefaultTables(freshUUID, tableRefs)

      jobManager <- JobManager[F, (UUID, UUID), Nothing]()

      lookupDestination: (UUID => F[Option[Destination[F]]]) = destinations.destinationOf(_).map(_.toOption)
      lookupTable: (UUID => F[Option[TableRef[SqlQuery]]]) = tableRefs.lookup

      runner = PushRunner(lookupTable, sqlEvaluator, lookupDestination, resultRender)

      push <- Resource.liftF(DefaultResultPush[F, UUID, UUID, SqlQuery, R](
        lookupTable,
        lookupDestination,
        jobManager,
        runner))

    } yield new Quasar(datasources, destinations, tables, sqlEvaluator, push)
  }

  ////

  private def warnDuplicates[F[_]: Sync, A, B: Show](l: List[A])(fn: A => B): F[Unit] =
    Sync[F].delay(l.groupBy(fn) foreach {
      case (b, grouped) =>
        if (grouped.length > 1) {
          log.warn(s"Found duplicate modules for type ${b.show}")
        }
    })

  private val rec = construction.RecFunc[Fix]

  private def reifiedAggregateDs[F[_]: Functor, G[_], P <: ResourcePathType]
      : Datasource[F, G, ?, CompositeResult[F, QueryResult[F]], P] ~> Datasource[F, G, ?, EvalResult[F], P] =
    new (Datasource[F, G, ?, CompositeResult[F, QueryResult[F]], P] ~> Datasource[F, G, ?, EvalResult[F], P]) {
      def apply[A](ds: Datasource[F, G, A, CompositeResult[F, QueryResult[F]], P]) = {
        val l = Datasource.ploaders[F, G, A, CompositeResult[F, QueryResult[F]], A, EvalResult[F], P]
        l.modify(_.map(_.map(reifyAggregateStructure)))(ds)
      }
    }

  private def reifyAggregateStructure[F[_], A](s: Stream[F, (ResourcePath, A)])
      : Stream[F, (ResourcePath, QSMap[Fix, A])] =
    s map { case (rp, a) =>
      (rp, QSMap(a, rec.Hole))
    }
}
