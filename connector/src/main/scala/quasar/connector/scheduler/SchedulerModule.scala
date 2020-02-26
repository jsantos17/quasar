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

package quasar.connector.scheduler

import slamdata.Predef._

import quasar.connector.MonadResourceErr

import java.util.UUID

import argonaut.Json

import cats.effect.{ConcurrentEffect, ContextShift, Timer}

trait SchedulerModule {
  def scheduler[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    config: Json)
      : F[Scheduler[F, UUID, UUID]]
}
