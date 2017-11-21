/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.mongodb

import quasar._
import quasar.common.{Map => _, _}
import quasar.contrib.pathy._
import quasar.contrib.specs2.PendingWithActualTracking
import quasar.ejson.{EJson, Fixed}
import quasar.javascript._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.workflow._
import quasar.qscript.ReduceFuncs
import quasar.sql.JoinDir
import slamdata.Predef._

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz._

class PlannerQScriptSpec extends
    PlannerHelpers with
    PendingWithActualTracking {

  import fixExprOp._
  import PlannerHelpers._
  import expr3_2Fp._
  import jscore._
  import CollectionUtil._
  import Reshape.reshape

  val dsl =
    quasar.qscript.construction.mkDefaults[Fix, fs.MongoQScript[Fix, ?]]
  import dsl._

  val json = Fixed[Fix[EJson]]

  //TODO make this independent of MongoQScript and move to a place where all
  //     connector tests can refer to it
  val simpleJoin: Fix[fs.MongoQScript[Fix, ?]] =
    fix.EquiJoin(
      fix.Unreferenced,
      free.Filter(
        free.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.ExcludeId),
        func.Guard(func.Hole, Type.AnyObject, func.Constant(json.bool(true)), func.Constant(json.bool(false)))),
      free.Filter(
        free.ShiftedRead[AFile](rootDir </> dir("db") </> file("smallZips"), qscript.ExcludeId),
        func.Guard(func.Hole, Type.AnyObject, func.Constant(json.bool(true)), func.Constant(json.bool(false)))),
      List((func.ProjectKeyS(func.Hole, "_id"), func.ProjectKeyS(func.Hole, "_id"))),
      JoinType.Inner,
      func.ProjectKeyS(func.RightSide, "city"))

  "plan from qscript" should {
    "plan simple join ($lookup)" in {
      qplan(simpleJoin) must beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("_id") -> Selector.Exists(true))),
        $project(reshape(JoinDir.Left.name -> $$ROOT)),
        $lookup(
          CollectionName("smallZips"),
          JoinHandler.LeftName \ BsonField.Name("_id"),
          BsonField.Name("_id"),
          JoinHandler.RightName),
        $unwind(DocField(JoinHandler.RightName)),
        $project(
          reshape(sigil.Quasar -> $field(JoinDir.Right.name, "city")),
          ExcludeId)))
    }

    "plan typechecks with JS when unable to extract ExprOp" in {
      qplan(
        fix.Filter(
          fix.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.IncludeId),
          func.Guard(
            func.ProjectKeyS(func.ProjectIndexI(func.Hole, 1), "parentid"),
            Type.Str,
            func.Constant(json.bool(false)),
            func.Constant(json.bool(true))))) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(reshape("0" -> $arrayLit(List($field("_id"), $$ROOT)))),
        $simpleMap(
          NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "0" ->
              If(Call(ident("isString"),
                List(
                  Select(Access(
                    Access(
                      ident("x"),
                      jscore.Literal(Js.Str("0"))),
                    jscore.Literal(Js.Num(1, false))), "parentid"))),
                jscore.Literal(Js.Bool(false)),
                jscore.Literal(Js.Bool(true))),
            "src" -> Access(ident("x"), jscore.Literal(Js.Str("0")))
          )))),
          ListMap()),
        $match(Selector.Doc(
          BsonField.Name("0") -> Selector.Eq(Bson.Bool(true))
        )),
        $project(reshape(sigil.Quasar -> $field("src")))))
    }

    "plan Reduce with Arbitrary on Hole" in {
      qplan(
        fix.Reduce(
          fix.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.ExcludeId),
          List(func.Hole),
          List(ReduceFuncs.Arbitrary(func.Hole)),
          func.ReduceIndex(0.right))) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
          jscore.Call(ident("remove"), List(ident("x"), jscore.Literal(Js.Str("_id"))))))),
          ListMap()),
        $group(
          Grouped.grouped(),
          -\/(reshape("0" -> $$ROOT))),
        $project(
          reshape(sigil.Quasar -> $field("_id", "0")),
          ExcludeId)))
    }

    "plan nested Reduce with Arbitrary on Hole" in {
      qplan(
        fix.Reduce(
          fix.Reduce(
            fix.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.ExcludeId),
            List(func.Hole),
            List(ReduceFuncs.Arbitrary(func.Hole)),
            func.ReduceIndex(0.right)),
          List(),
          List(ReduceFuncs.Count(func.Hole)),
          func.ReduceIndex(0.right))) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
          jscore.Call(ident("remove"), List(ident("x"), jscore.Literal(Js.Str("_id"))))))),
          ListMap()),
        $group(
          Grouped.grouped(),
          -\/(reshape("0" -> $$ROOT))),
        $group(
          Grouped.grouped("f0" -> $sum($literal(Bson.Int32(1)))),
          \/-($literal(Bson.Null))),
        $project(
          reshape(sigil.Quasar -> $field("f0")),
          ExcludeId)))
    }

    "plan sorted Reduce with sort key not in Reduce" in {
      qplan(
        fix.Map(
          fix.Sort(
            fix.Reduce(
              fix.Sort(
                fix.Map(
                  fix.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.ExcludeId),
                  func.ConcatMaps(
                    func.MakeMap(func.Constant(json.str("city")), func.ProjectKey(func.Hole, func.Constant(json.str("city")))),
                    func.MakeMap(func.Constant(json.str("__sd__0")), func.ProjectKey(func.Hole, func.Constant(json.str("pop")))))),
                List(),
                NonEmptyList((func.ProjectKeyS(func.Hole, "__sd__0"), SortDir.Descending))),
              List(func.MakeMap(func.Constant(json.str("city")), func.ProjectKey(func.Hole, func.Constant(json.str("city"))))),
              List(ReduceFuncs.First(func.Hole)),
              func.ConcatMaps(
                func.MakeMap(func.Constant(json.str("0")), func.ProjectKeyS(func.ReduceIndex(0.right), "__sd__0")),
                func.MakeMap(func.Constant(json.str("city")), func.ProjectKeyS(func.ReduceIndex(0.left), "city")))),
            List(),
            NonEmptyList((func.ProjectKeyS(func.Hole, "__sd__0"), SortDir.Descending))),
          func.MakeMap(func.Constant(json.str("city")), func.ProjectKeyS(func.Hole, "city")))) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "city"    -> $field("city"),
            "__sd__0" -> $field("pop")),
          ExcludeId),
        $sort(NonEmptyList(
          BsonField.Name("__sd__0") -> SortDir.Descending)),
        $group(
          Grouped.grouped(),
          -\/(reshape("0" -> $objectLit(ListMap(
            BsonField.Name("city") -> $field("city")))))),
        $project(
          reshape(
            "city" -> $field("_id", "0", "city")),
          ExcludeId),
        $sort(NonEmptyList(
          BsonField.Name("__sd__0") -> SortDir.Descending)),
        $project(
          reshape("city" -> $field("city")),
          ExcludeId)))
    }

    "plan sorted Reduce with sort key in Reduce" in {
      qplan(
        fix.Sort(
          fix.Reduce(
            fix.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.ExcludeId),
            List(func.ProjectKeyS(func.Hole, "city")),
            List(ReduceFuncs.Arbitrary(func.ProjectKeyS(func.Hole, "city"))),
            func.ReduceIndex(0.right)),
          List(),
          NonEmptyList((func.Hole, SortDir.Ascending)))) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          Grouped.grouped(),
          -\/(reshape("0" -> $field("city")))),
        $project(
          // FIXME: remove duplicates
          reshape(
            "0"   -> $field("_id", "0"),
            "src" -> $field("_id", "0")),
          ExcludeId),
        $sort(NonEmptyList(BsonField.Name("0") -> SortDir.Ascending)),
        $project(
          reshape(sigil.Quasar -> $field("src")),
          ExcludeId)))
    }
  }
}
