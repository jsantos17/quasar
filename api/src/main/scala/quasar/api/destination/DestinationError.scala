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

package quasar.api.destination

import slamdata.Predef._

import monocle.Prism
import scalaz.{ISet, NonEmptyList}

sealed trait DestinationError[I, C] extends Product with Serializable

object DestinationError {
  sealed trait CreateError extends DestinationError[Nothing, Nothing]
  final case class DestinationUnsupported(kind: DestinationType, supported: ISet[DestinationType])
      extends CreateError
  final case class DestinationNameExists(name: DestinationName)
      extends CreateError

  sealed trait InitializationError[C] extends DestinationError[Nothing, C]
  final case class MalformedConfiguration[C](kind: DestinationType, config: C, reason: String)
      extends InitializationError[C]
  final case class InvalidConfiguration[C](kind: DestinationType, config: C, reasons: NonEmptyList[String])
      extends InitializationError[C]
  final case class ConnectionFailed[C](kind: DestinationType, config: C, cause: Exception)
      extends InitializationError[C]
  final case class AccessDenied[C](kind: DestinationType, config: C, reason: String)
      extends InitializationError[C]

  sealed trait ExistentialError[I] extends DestinationError[I, Nothing]
  final case class DestinationNotFound[I](destinationId: I)
      extends ExistentialError[I]

  def destinationUnsupported[E >: CreateError <: DestinationError[_, _]]
      : Prism[E, (DestinationType, ISet[DestinationType])] =
    Prism.partial[E, (DestinationType, ISet[DestinationType])] {
      case DestinationUnsupported(k, s) => (k, s)
    } (DestinationUnsupported.tupled)

  def destinationNameExists[E >: CreateError <: DestinationError[_, _]]
      : Prism[E, DestinationName] =
    Prism.partial[E, DestinationName] {
      case DestinationNameExists(n) => n
    } (DestinationNameExists(_))

  def malformedConfiguration[C, E >: InitializationError[C] <: DestinationError[_, C]]
      : Prism[E, (DestinationType, C, String)] =
    Prism.partial[E, (DestinationType, C, String)] {
      case MalformedConfiguration(d, c, r) => (d, c, r)
    } ((MalformedConfiguration[C] _).tupled)

  def invalidConfiguration[C, E >: InitializationError[C] <: DestinationError[_, C]]
      : Prism[E, (DestinationType, C, NonEmptyList[String])] =
    Prism.partial[E, (DestinationType, C, NonEmptyList[String])] {
      case InvalidConfiguration(d, c, rs) => (d, c, rs)
    } ((InvalidConfiguration[C] _).tupled)

  def connectionFailed[C, E >: InitializationError[C] <: DestinationError[_, C]]
      : Prism[E, (DestinationType, C, Exception)] =
    Prism.partial[E, (DestinationType, C, Exception)] {
      case ConnectionFailed(d, c, e) => (d, c, e)
    } ((ConnectionFailed[C] _).tupled)

  def accessDenied[C, E >: InitializationError[C] <: DestinationError[_, C]]
      : Prism[E, (DestinationType, C, String)] =
    Prism.partial[E, (DestinationType, C, String)] {
      case AccessDenied(d, c, e) => (d, c, e)
    } ((AccessDenied[C] _).tupled)

  def destinationNotFound[I, E >: ExistentialError[I] <: DestinationError[I, _]]
      : Prism[E, I] =
    Prism.partial[E, I] {
      case DestinationNotFound(i) => i
    } (DestinationNotFound[I](_))
}
