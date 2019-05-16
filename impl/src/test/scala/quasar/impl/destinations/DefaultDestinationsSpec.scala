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

import slamdata.Predef.{Stream => _, _}

import quasar.Condition
import quasar.api.destination.{DestinationError, DestinationMeta, DestinationName, DestinationRef, DestinationType, Destinations}
import quasar.connector.{Destination, DestinationModule, ResourceError}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.RefIndexedStore

import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import cats.effect.IO
import cats.effect.concurrent.Ref
import eu.timepit.refined.auto._
import fs2.Stream
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.unzip._
import scalaz.{IMap, ISet}
import shims._

object DefaultDestinationsSpec extends quasar.Qspec {
  implicit val cs = IO.contextShift(global)
  implicit val tmr = IO.timer(global)
  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  def freshId(ref: Ref[IO, Int]): IO[Int] =
    ref.get.map(_ + 1)

  val mockModules: IMap[DestinationType, DestinationModule] =
    IMap(MockDestinationModule.destinationType -> MockDestinationModule)

  def manager(
    running: IMap[Int, (Destination[IO], IO[Unit])],
    errors: IMap[Int, Exception],
    modules: IMap[DestinationType, DestinationModule]): IO[DestinationManager[Int, Json, IO]] =
    (Ref[IO].of(running) |@| Ref[IO].of(errors))(
      DefaultDestinationManager[Int, IO](modules, _, _))

  def mkDestinations(
    store: IMap[Int, DestinationRef[Json]],
    running: IMap[Int, (Destination[IO], IO[Unit])],
    errors: IMap[Int, Exception]): IO[Destinations[IO, Stream[IO, ?], Int, Json]] =
    for {
      initialIdRef <- Ref.of[IO, Int](0)
      fId = freshId(initialIdRef)
      storeRef <- Ref[IO].of(store)
      store = RefIndexedStore[Int, DestinationRef[Json]](storeRef)
      mgr <- manager(running, errors, mockModules)
    } yield DefaultDestinations[IO, Int, Json](fId, store, mgr)

  def emptyDestinations: IO[Destinations[IO, Stream[IO, ?], Int, Json]] =
    mkDestinations(IMap.empty, IMap.empty, IMap.empty)

  val MockDestinationType = MockDestinationModule.destinationType

  val testRef =
    DestinationRef(
      MockDestinationType,
      DestinationName("foo_mock"),
      Json.jEmptyString)

  "destinations" >> {
    "add destination" >> {
      "creates and saves destinations" >> {
        val testRun = for {
          dests <- emptyDestinations
          result <- dests.addDestination(testRef)
          foundRef <- result.toOption.map(dests.destinationRef(_)).sequence
        } yield (result, foundRef)

        val (res, found) = testRun.unsafeRunSync

        res.toEither must beRight
        found must beSome
      }

      "rejects duplicate names" >> {
        val testRun = for {
          dests <- emptyDestinations
          original <- dests.addDestination(testRef)
          duplicate <- dests.addDestination(testRef)
        } yield (original, duplicate)

        val (original, duplicate) = testRun.unsafeRunSync

        original.toEither must beRight
        duplicate.toEither must beLeft(
          DestinationError.destinationNameExists(DestinationName("foo_mock")))
      }

      "rejects unknown destination" >> {
        val unknownType = DestinationType("unknown", 1L, 1L)
        val unknownRef = DestinationRef.kind.set(unknownType)(testRef)

        val testRun = for {
          dests <- emptyDestinations
          addResult <- dests.addDestination(unknownRef)
        } yield addResult

        testRun.unsafeRunSync.toEither must beLeft(
          DestinationError.destinationUnsupported(unknownType, ISet.singleton(MockDestinationType)))
      }
    }

    "destination status" >> {
      "returns an error for an unknown destination" >> {
        val testRun = for {
          dests <- emptyDestinations
          res <- dests.destinationStatus(42)
        } yield res

        testRun.unsafeRunSync.toEither must beLeft(DestinationError.destinationNotFound(42))
      }

      "return a normal condition when no errors are present" >> {
        val testRun = for {
          dests <- mkDestinations(IMap(1 -> testRef), IMap.empty, IMap.empty)
          res <- dests.destinationStatus(1)
        } yield res

        testRun.unsafeRunSync.toEither must beRight(Condition.normal[Exception]())
      }

      "returns error for destinations with errors" >> {
        val ex = new RuntimeException("oh noes")
        val testRun = for {
          dests <- mkDestinations(IMap(1 -> testRef), IMap.empty, IMap(1 -> ex))
          res <- dests.destinationStatus(1)
        } yield res

        testRun.unsafeRunSync.toEither must beRight(Condition.abnormal(ex))
      }
    }

    "destination metadata" >> {
      "includes exception when a destination has errored" >> {
        val ex = new RuntimeException("oh noes")

        val testRun = for {
          dests <- mkDestinations(IMap(1 -> testRef), IMap.empty, IMap(1 -> ex))
          st <- dests.allDestinationMetadata
          allMeta <- st.compile.toList
        } yield allMeta.seconds

        testRun.unsafeRunSync must_== List(
          DestinationMeta(
            MockDestinationModule.destinationType,
            DestinationName("foo_mock"),
            Condition.abnormal(ex)))
      }
    }

    "destination removal" >> {
      "removes a destination" >> {
        val testRun = for {
          dests <- mkDestinations(IMap(10 -> testRef), IMap.empty, IMap.empty)
          removed <- dests.removeDestination(10)
          retrieved <- dests.destinationRef(10)
        } yield (removed, retrieved)

        val (removed, retrieved) = testRun.unsafeRunSync

        removed must_== Condition.normal()
        retrieved.toEither must beLeft(DestinationError.destinationNotFound(10))
      }
    }

    "destination lookup" >> {
      "returns sanitized refs" >> {
        val testRun = for {
          dests <- emptyDestinations
          addResult <- dests.addDestination(testRef)
          found <- addResult.toOption.map(dests.destinationRef(_)).sequence
          foundRef = found >>= (_.toOption)
        } yield foundRef

        testRun.unsafeRunSync must beSome(
          DestinationRef.config.set(Json.jString("sanitized"))(testRef))
      }
    }
  }
}