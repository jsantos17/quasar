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

package quasar.run

import slamdata.Predef._
import quasar.{Variables, VarName, VarValue}
import quasar.api.datasource._
import quasar.api.table.{TableName, TableRef}
import quasar.contrib.pathy.prismADir
import quasar.fp.numeric.Positive
import quasar.mimir.storage.StoreKey
import quasar.precog.common._
import quasar.sql.Query

import java.util.UUID

import argonaut._, Argonaut._
import eu.timepit.refined.refineV
import monocle.Prism
import scalaz.\/
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.traverse._

object optics {

  val DsRefTypeField = "type"
  val DsRefTypeVersionField = "typeVersion"
  val DsRefNameField = "name"
  val DsRefConfigField = "config"

  def rValueDatasourceRefP[A](cfgP: Prism[RValue, A]): Prism[RValue, DatasourceRef[A]] = {
    def fromRValue(rv: RValue): Option[DatasourceRef[A]] =
      for {
        kind <-
          RValue.rField1(DsRefTypeField)
            .composePrism(RValue.rString)
            .getOption(rv)

        ver <-
          RValue.rField1(DsRefTypeVersionField)
            .composePrism(RValue.rNum)
            .getOption(rv)

        name <-
          RValue.rField1(DsRefNameField)
            .composePrism(RValue.rString)
            .getOption(rv)

        cfg <-
          RValue.rField1(DsRefConfigField)
            .composePrism(cfgP)
            .getOption(rv)

        tname <- refineV[DatasourceType.NameP](kind).toOption

        tver <- Positive(ver.longValue)

        dsType = DatasourceType(tname, tver)
        dsName = DatasourceName(name)
      } yield DatasourceRef(dsType, dsName, cfg)

    val toRValue: DatasourceRef[A] => RValue = {
      case DatasourceRef(DatasourceType(t, v), DatasourceName(n), c) =>
        RValue.rObject(Map(
          DsRefTypeField -> CString(t.value),
          DsRefTypeVersionField -> CNum(v.value),
          DsRefNameField -> CString(n),
          DsRefConfigField -> cfgP(c)))
    }

    Prism(fromRValue)(toRValue)
  }

  val TableRefNameField = "name"
  val TableRefQueryField = "query"

  def rValueTableRefP[A](queryP: Prism[RValue, A]): Prism[RValue, TableRef[A]] = {
    def fromRValue(rv: RValue): Option[TableRef[A]] =
      for {
        name <-
          RValue.rField1(TableRefNameField)
            .composePrism(RValue.rString)
            .getOption(rv)
        query <-
          RValue.rField1(TableRefQueryField)
            .composePrism(queryP)
            .getOption(rv)
      } yield TableRef(TableName(name), query)

    val toRValue: TableRef[A] => RValue = {
      case TableRef(TableName(name), query) =>
        RValue.rObject(Map(
          TableRefNameField -> CString(name),
          TableRefQueryField -> queryP(query)))
    }

    Prism(fromRValue)(toRValue)
  }

  val SqlQueryQueryField = "query"
  val SqlQueryVarsField = "vars"
  val SqlQueryPathField = "path"

  val rValueSqlQueryP: Prism[RValue, SqlQuery] = {
    def fromRValue(rv: RValue): Option[SqlQuery] =
      for {
        query <-
          RValue.rField1(SqlQueryQueryField)
            .composePrism(RValue.rString)
            .getOption(rv)
        vars0 <-
          RValue.rField1(SqlQueryVarsField)
            .composePrism(RValue.rObject)
            .getOption(rv)
        vars <- {
          val opts: List[Option[(VarName, VarValue)]] = vars0.toList.map {
            case (k, v) => RValue.rString.getOption(v).map(s => (VarName(k), VarValue(s)))
          }
          opts.sequence.map(_.toMap)
        }
        path <-
          RValue.rField1(SqlQueryPathField)
            .composePrism(RValue.rString)
            .composePrism(prismADir)
            .getOption(rv)
      } yield SqlQuery(Query(query), Variables(vars), path)

    val toRValue: SqlQuery => RValue = {
      case SqlQuery(Query(query), Variables(vars), path) =>
        RValue.rObject(Map(
          SqlQueryQueryField -> CString(query),
          SqlQueryVarsField -> RValue.rObject(
            vars map {
              case (VarName(name), VarValue(value)) => (name, CString(value))
            }),
          SqlQueryPathField -> RValue.rString.composePrism(prismADir).reverseGet(path)))
    }

    Prism(fromRValue)(toRValue)
  }

  val rValueJsonP: Prism[RValue, Json] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def toRValue(js: Json): RValue =
      js.fold(
        jsonNull = RValue.rNull(),
        jsonBool = RValue.rBoolean(_),
        jsonNumber = n => RValue.rNum(n.toBigDecimal),
        jsonString = RValue.rString(_),
        jsonArray = elems => RValue.rArray(elems.map(toRValue)),
        jsonObject = fields => RValue.rObject(fields.toMap.mapValues(toRValue)))

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def fromRValue(rv: RValue): Option[Json] =
      rv match {
        case CEmptyObject => some(jEmptyObject)
        case RObject(fields) =>
          fields.toList.traverse({
            case (k, v) => fromRValue(v) strengthL k
          }).map(jObjectAssocList)

        case CEmptyArray => some(jEmptyArray)
        case RArray(elems) => elems.traverse(fromRValue).map(jArray)

        case CNull => some(jNull)
        case CBoolean(b) => some(jBool(b))
        case CString(s) => some(jString(s))
        case CLong(l) => some(jNumber(l))
        case CDouble(d) => some(jNumber(d))
        case CNum(n) => some(jNumber(JsonBigDecimal(n)))

        case _ => none
      }

    Prism(fromRValue)(toRValue)
  }

  val stringUuidP: Prism[String, UUID] =
    Prism[String, UUID](
      s => \/.fromTryCatchNonFatal(UUID.fromString(s)).toOption)(
      u => u.toString)

  val storeKeyUuidP: Prism[StoreKey, UUID] =
    StoreKey.stringIso composePrism stringUuidP
}
