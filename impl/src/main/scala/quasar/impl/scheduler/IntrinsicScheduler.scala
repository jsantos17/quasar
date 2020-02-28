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

package quasar.impl.scheduler

import slamdata.Predef._

import fs2.job.{Job, JobManager}

import quasar.Condition
import quasar.api.push.PushRunner
import quasar.api.scheduler.SchedulerError
import quasar.connector.scheduler.{Scheduler, PushSpec}

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.data.EitherT
import cats.implicits._

import cron4s.Cron

import eu.timepit.fs2cron.awakeEveryCron

import java.lang.Throwable

import fs2.Stream

final case class IntrinsicScheduler[F[_]: ConcurrentEffect: ContextShift: Timer, T, D, I](
  runner: PushRunner[F, T, D],
  jobManager: JobManager[F, I, Throwable],
  nextId: F[I]) extends Scheduler[F, T, D, I] {

  def schedule(schedule: String, spec: PushSpec[T, D]): F[Either[SchedulerError[T, D], I]] = {
    val sch = for {
      parsedSchedule <- EitherT.fromEither[F](
        Cron.parse(schedule).leftMap(details => SchedulerError.invalidSchedule(schedule, details)))

      str <- startSpec(spec)

      scheduled = awakeEveryCron(parsedSchedule) >> str.attempt

      id <- EitherT.right[SchedulerError[T, D]](nextId)

      job = Job(id, scheduled)

      submitted <- EitherT.right[SchedulerError[T, D]](jobManager.submit(job))

      _ <- if (submitted)
        EitherT.pure[F, SchedulerError[T, D]](())
      else
        EitherT.leftT[F, Unit](SchedulerError.scheduleFailed[T, D])
    } yield id

    sch.value
  }

  def removeSchedule(id: I): F[Unit] =
    jobManager.cancel(id)

  private def startSpec(spec: PushSpec[T, D])
      : EitherT[F, SchedulerError[T, D], Stream[F, Unit]] =
    EitherT(
      runner.run(
        spec.tableId,
        spec.columns,
        spec.destinationId,
        spec.path,
        spec.resultType,
        spec.limit)).leftMap(err => (SchedulerError.PushError(err): SchedulerError[T, D]))
}
