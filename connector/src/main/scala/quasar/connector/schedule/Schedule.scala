/*
 * Copyright 2020 Precog Data
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

package quasar.connector.schedule

import slamdata.Predef._

import quasar.Condition
import quasar.api.schedule.ScheduleError._

import fs2.Stream

trait Schedule[F[_], C, I] {
  def intentions: Stream[F, (I, C)]
  def addIntention(config: C): F[Either[IncorrectIntention[C], I]]
  def getIntention(i: I): F[Either[IntentionNotFound[I], C]]
  def editIntention(i: I, config: C): F[Condition[IntentionError[C, I]]]
  def deleteIntention(i: I): F[Condition[IntentionNotFound[I]]]
}
