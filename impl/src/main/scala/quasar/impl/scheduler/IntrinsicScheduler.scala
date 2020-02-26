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

import quasar.api.push.ResultPush
import quasar.connector.scheduler.{Scheduler, PushSpec}

import cats.effect.{ConcurrentEffect, ContextShift, Timer}

final class IntrinsicScheduler[F[_]: ConcurrentEffect: ContextShift: Timer, T, D](
  resultPush: ResultPush[F, T, D]) extends Scheduler[F, T, D] {

  type Id = UUID

  def schedule(schedule: String, spec: PushSpec[T, D]): F[Id] = ???

  def removeSchedule(id: Id): F[Unit] = ???

}
