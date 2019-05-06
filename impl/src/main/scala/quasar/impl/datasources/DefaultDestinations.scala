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

import quasar.api.destination._
import quasar.impl.storage.IndexedStore

import cats.effect.Sync
import fs2.Stream
import scalaz.Equal

// TODO: implement me
abstract class DefaultDestinations[F[_]: Sync, I: Equal, C: Equal] private (
    freshId: F[I],
    refs: IndexedStore[F, I, DestinationRef[C]])
    extends Destinations[F, Stream[F, ?], I, C] {}
