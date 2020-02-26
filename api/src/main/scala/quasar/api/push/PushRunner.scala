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

package quasar.api.push

import slamdata.Predef._

import quasar.api.Column
import quasar.api.resource.ResourcePath

import cats.data.NonEmptyList

import fs2.Stream

trait PushRunner[F[_], T, D] {
  def run(
    tableId: T,
    columns: NonEmptyList[Column[SelectedType]],
    destinationId: D,
    path: ResourcePath,
    format: ResultType,
    limit: Option[Long]): F[Either[NonEmptyList[ResultPushError[T, D]], Stream[F, Unit]]]
}
