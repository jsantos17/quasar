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

package quasar.api.scheduler

import slamdata.Predef._


import quasar.api.push.ResultPushError

import cats.data.NonEmptyList

sealed trait SchedulerError[+T, +D] extends Product with Serializable

object SchedulerError {
  case class InvalidSchedule(expr: String) extends SchedulerError[Nothing, Nothing]
  case object ScheduleFailed extends SchedulerError[Nothing, Nothing]
  final case class PushError[T, D](errs: NonEmptyList[ResultPushError[T, D]])
      extends SchedulerError[T, D]

  def scheduleFailed[T, D]: SchedulerError[T, D] =
    ScheduleFailed

  def invalidSchedule[T, D](expr: String): SchedulerError[T, D] =
    InvalidSchedule(expr)

}
