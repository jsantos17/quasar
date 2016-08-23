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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.ejson.EJson
import quasar.fp._
import quasar.physical.marklogic.ejson.AsXQuery
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript.{MapFunc, MapFuncs, Nullary}, MapFuncs._

import matryoshka._, Recursive.ops._
import scalaz.syntax.show._

object MapFuncPlanner {
  import expr.if_

  def apply[T[_[_]]: Recursive: ShowT]: Algebra[MapFunc[T, ?], XQuery] = {
    case Nullary(ejson) => ejson.cata(AsXQuery[EJson].asXQuery)

    // array
    case Length(arr) => fn.count(arr)

    // time TODO

    // math
    case Negate(x) => -x
    case Add(x, y) => x + y
    case Multiply(x, y) => x * y
    case Subtract(x, y) => x - y
    case Divide(x, y) => x div y
    case Modulo(x, y) => x mod y
    case Power(b, e) => math.pow(b, e)

    // relations
    // TODO: When to use value vs. general comparisons?
    case Not(x) => fn.not(x)
    case Eq(x, y) => x eq y
    case Neq(x, y) => x ne y
    case Lt(x, y) => x lt y
    case Lte(x, y) => x le y
    case Gt(x, y) => x gt y
    case Gte(x, y) => x ge y
    case And(x, y) => x and y
    case Or(x, y) => x or y
    case Cond(p, t, f) => if_ (p) then_ t else_ f

    // string
    case Lower(s) => fn.lowerCase(s)
    case Upper(s) => fn.upperCase(s)
    case ToString(x) => fn.string(x)

    // other
    case Range(x, y) => x to y

    case mapFunc => s"(: ${mapFunc.shows} :)".xqy
  }
}
