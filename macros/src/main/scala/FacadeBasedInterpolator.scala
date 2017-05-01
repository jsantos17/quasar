/*
 * Copyright 2014–2016 SlamData Inc.
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

package ygg.macros

import slamdata.Predef._
import scala.{ Any, StringContext }
import jawn.Facade

class FacadeBasedInterpolator[A] {
  implicit class Interpolator(sc: StringContext)(implicit val facade: Facade[A]) {
    def json(args: Any*): A             = macro JawnFacadeMacros.singleImpl[A]
    def jsonMany(args: Any*): Vector[A] = macro JawnFacadeMacros.manyImpl[A]
  }
}

object JsonMacros {
  object EJson extends FacadeBasedInterpolator[quasar.ejson.EJson[quasar.Data]]
  object Argonaut extends FacadeBasedInterpolator[argonaut.Json] {
    implicit def argonautFacade = argonaut.JawnParser.facade
  }
  object Jawn extends FacadeBasedInterpolator[jawn.ast.JValue] {
    implicit def janwFacade = jawn.ast.JawnFacade
  }
}