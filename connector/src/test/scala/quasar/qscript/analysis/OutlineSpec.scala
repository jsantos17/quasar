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

package quasar.qscript.analysis

import slamdata.Predef.{Map => _, _}
import quasar.common.{JoinType, JoinTypeArbitrary, SortDir}
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.ejson
import quasar.ejson.{CommonEJson, EJson, EJsonArbitrary, ExtEJson}
import quasar.ejson.implicits._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data.Fix
import matryoshka.data.free._
import matryoshka.implicits._
import org.specs2.scalacheck._
import scalaz._, Scalaz._

final class OutlineSpec extends quasar.Qspec with QScriptHelpers {
  import EJsonArbitrary._, FigureArbitrary._, JoinTypeArbitrary._
  import MapFuncCore.StaticMap
  import Outline.{Figure, Shape, freeEJsonEqual, undefinedF, unknownF, arrF, mapF}

  implicit val params = Parameters(maxSize = 10)

  val toFree = convertToFree[EJson, Figure](_: Fix[EJson])
  val rollS = Free.roll[EJson, Figure] _

  val joinFunc =
    ConcatMapsR(
      MakeMapR(ConstantR(Fix(CommonEJson(ejson.Str("left")))), LeftSideF),
      MakeMapR(ConstantR(Fix(CommonEJson(ejson.Str("right")))), RightSideF))

  val modSides: (Shape, Shape) => Shape =
    (l, r) => rollS(ExtEJson(ejson.Map(List(
      rollS(CommonEJson(ejson.Str("left"))) -> l,
      rollS(CommonEJson(ejson.Str("right"))) -> r))))

  "Outline FreeMap" >> {
    val outlineFM = Outline.outlineF(_: FreeMap)(κ(unknownF))
    val av = ConcatArraysR(HoleF, HoleF)
    val mv = ConcatMapsR(HoleF, HoleF)
    val uv = ProjectIndexR(HoleF, ConstantR(ejsonInt(42)))

    "unknown when no static structure" >> {
      outlineFM(uv) must_= unknownF
    }

    "constants are lifted into Free" >> prop { ejs: Fix[EJson] =>
      outlineFM(ConstantR(ejs)).transCataM[Option, Fix[EJson], EJson](_.run.toOption) must_= Some(ejs)
    }

    "map construction with constant keys result in maps" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm = ConcatMapsR(MakeMapR(ConstantR(k1), av), MakeMapR(ConstantR(k2), mv))

      val ss =
        rollS(ExtEJson(ejson.Map(List(
          toFree(k1) -> arrF,
          toFree(k2) -> mapF))))

      outlineFM(fm) must_= ss
    }}

    "map construction with at least one non-constant key is not static" >> prop { k1: Fix[EJson] =>
      val k2 = ProjectFieldR(HoleF, ConstantR(ejsonStr("fieldName")))
      val fm = ConcatMapsR(MakeMapR(ConstantR(k1), uv), MakeMapR(k2, uv))

      outlineFM(fm) must_= mapF
    }

    "concatenation of maps is right-biased" >> prop { k: Fix[EJson] =>
      val fm = ConcatMapsR(MakeMapR(ConstantR(k), mv), MakeMapR(ConstantR(k), av))
      val ss = rollS(ExtEJson(ejson.Map(List(toFree(k) -> arrF))))

      outlineFM(fm) must_= ss
    }

    "concatenation of maps preserves field order" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson], k3: Fix[EJson]) => (k1 =/= k2 && k1 =/= k3 && k2 =/= k3) ==> {

      val l =
        StaticMap[Fix, Hole](List(k1 -> HoleF, k2 -> HoleF, k3 -> HoleF))

      val r =
        StaticMap[Fix, Hole](List(k2 -> av))

      val ss = rollS(ExtEJson(ejson.Map(List(
        toFree(k1) -> unknownF,
        toFree(k2) -> arrF,
        toFree(k3) -> unknownF))))

      outlineFM(ConcatMapsR(l, r)) must_= ss
    }}

    "field projection on static map results in field value" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm =
        ProjectFieldR(
          ConcatMapsR(MakeMapR(ConstantR(k1), mv), MakeMapR(ConstantR(k2), av)),
          ConstantR(k1))

      outlineFM(fm) must_= mapF
    }}

    "field projection on static map without field results in undefined" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm = ProjectFieldR(MakeMapR(ConstantR(k1), av), ConstantR(k2))

      outlineFM(fm) must_= undefinedF
    }}

    "field deletion on static map results in static field deleted" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm =
        DeleteFieldR(
          ConcatMapsR(MakeMapR(ConstantR(k2), av), MakeMapR(ConstantR(k1), mv)),
          ConstantR(k1))

      val ss =
        rollS(ExtEJson(ejson.Map(List(toFree(k2) -> arrF))))

      outlineFM(fm) must_= ss
    }}

    "field deletion of nonexistent on static map is identity" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm = DeleteFieldR(MakeMapR(ConstantR(k1), av), ConstantR(k2))

      val ss = rollS(ExtEJson(ejson.Map(List(toFree(k1) -> arrF))))

      outlineFM(fm) must_= ss
    }}

    "array construction results in arrays" >> {
      val fm =
        ConcatArraysR(
          MakeArrayR(uv),
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(mv),
              MakeArrayR(av)),
            MakeArrayR(uv)))

      val ss =
        rollS(CommonEJson(ejson.Arr(List(unknownF, mapF, arrF, unknownF))))

      outlineFM(fm) must_= ss
    }

    "index projection on static array results in index value" >> {
      val fm =
        ProjectIndexR(
          ConcatArraysR(ConcatArraysR(MakeArrayR(av), MakeArrayR(mv)), MakeArrayR(uv)),
          ConstantR(ejsonInt(1)))

      outlineFM(fm) must_= mapF
    }

    "index projection on static array with invalid index results in undefined" >> {
      outlineFM(ProjectIndexR(MakeArrayR(mv), ConstantR(ejsonInt(-5)))) must_= undefinedF
    }

    "index projection on static array with nonexistent index results in undefined" >> {
      outlineFM(ProjectIndexR(MakeArrayR(mv), ConstantR(ejsonInt(7)))) must_= undefinedF
    }

    "complex shape" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson]) => (k1 =/= k2) ==> {

      val fm =
        ConcatArraysR(
          MakeArrayR(uv),
          MakeArrayR(ConcatMapsR(
            MakeMapR(ConstantR(k1), MakeArrayR(mv)),
            MakeMapR(ConstantR(k2), MakeMapR(ConstantR(k1), av)))))

      val ss =
        rollS(CommonEJson(ejson.Arr(List(
          unknownF,
          rollS(ExtEJson(ejson.Map(List(
            toFree(k1) -> rollS(CommonEJson(ejson.Arr(List(mapF)))),
            toFree(k2) -> rollS(ExtEJson(ejson.Map(List(toFree(k1) -> arrF))))))))))))

      outlineFM(fm) must_= ss
    }}
  }

  "Outline QScriptCore" >> {
    val outlineQC: QScriptCore[Shape] => Shape =
      Outline[QScriptCore].outlineƒ

    val func =
      ConcatArraysR(
        MakeArrayR(HoleF),
        MakeArrayR(ConstantR(Fix(ExtEJson(ejson.Int(75))))))

    val modSrc: Shape => Shape =
      s => rollS(CommonEJson(ejson.Arr(List(s, rollS(ExtEJson(ejson.Int(75)))))))

    "Map is shape of function applied to source" >> prop { srcShape: Shape =>
      outlineQC(Map(srcShape, func)) must_= modSrc(srcShape)
    }

    "LeftShift(IncludeId) results in shape of repair applied to static array" >> prop { srcShape: Shape =>
      val r = rollS(CommonEJson(ejson.Arr(List(unknownF, unknownF))))
      outlineQC(LeftShift(srcShape, HoleF, IncludeId, joinFunc)) must_= modSides(srcShape, r)
    }

    "RightSide of repair is unknown when not IncludeId" >> prop { srcShape: Shape =>
      outlineQC(LeftShift(srcShape, HoleF, ExcludeId, joinFunc)) must_= modSides(srcShape, unknownF)
    }

    "Reduce tracks static input shape through buckets" >> prop { srcShape: Shape =>
      val rfunc = joinFunc map {
        case LeftSide => ReduceIndex(0.left)
        case RightSide => ReduceIndex(0.right)
      }

      val outShape = modSides(modSrc(srcShape), unknownF)

      outlineQC(Reduce(srcShape, List(func), List(ReduceFuncs.Count(HoleF)), rfunc)) must_= outShape
    }

    "Reduce tracks static input shape through parametric reducers" >> prop { srcShape: Shape =>
      val rfunc = joinFunc map {
        case LeftSide => ReduceIndex(0.right)
        case RightSide => ReduceIndex(1.right)
      }

      val reducers = List[ReduceFunc[FreeMap]](
        ReduceFuncs.Count(HoleF),
        ReduceFuncs.Last(func))

      val outShape = modSides(unknownF, modSrc(srcShape))

      outlineQC(Reduce(srcShape, Nil, reducers, rfunc)) must_= outShape
    }

    "Shape of composite-typed reducers is composite type" >> prop { srcShape: Shape =>
      val rfunc = joinFunc map {
        case LeftSide => ReduceIndex(0.right)
        case RightSide => ReduceIndex(1.right)
      }

      val reducers = List[ReduceFunc[FreeMap]](
        ReduceFuncs.UnshiftArray(HoleF),
        ReduceFuncs.UnshiftMap(HoleF, HoleF))

      val outShape = modSides(arrF, mapF)

      outlineQC(Reduce(srcShape, Nil, reducers, rfunc)) must_= outShape
    }

    "Subset is the shape of `from` applied to the source shape" >> prop { srcShape: Shape =>
      val from: FreeQS =
        Free.roll(QCT(Sort(
          Free.roll(QCT(Map(HoleQS, func))),
          Nil,
          NonEmptyList((HoleF, SortDir.Ascending)))))

      val count: FreeQS =
        Free.roll(QCT(Map(HoleQS, ConstantR(Fix(ExtEJson(ejson.Int(2)))))))

      outlineQC(Subset(srcShape, from, Take, count)) must_= modSrc(srcShape)
    }

    "Sort does not affect shape" >> prop { srcShape: Shape =>
      outlineQC(Sort(srcShape, Nil, NonEmptyList((func, SortDir.Descending)))) must_= srcShape
    }

    "Filter does not affect shape" >> prop { srcShape: Shape =>
      outlineQC(Filter(srcShape, func)) must_= srcShape
    }

    "Unreferenced has undefined shape" >> {
      outlineQC(Unreferenced()) must_= undefinedF
    }
  }

  val lfn: FreeMap = MapFuncCore.StaticArray(List(HoleF, ConstantR(ejsonInt(53))))
  val l: FreeQS = Free.roll(QCT(Map(HoleQS, lfn)))
  def lShape(srcShape: Shape): Shape =
    rollS(CommonEJson(ejson.Arr(List(srcShape, rollS(ExtEJson(ejson.Int(53)))))))

  val rfn: FreeMap = MapFuncCore.StaticArray(List(ConstantR(ejsonInt(78)), HoleF))
  val r: FreeQS = Free.roll(QCT(Map(HoleQS, rfn)))
  def rShape(srcShape: Shape): Shape =
    rollS(CommonEJson(ejson.Arr(List(rollS(ExtEJson(ejson.Int(78))), srcShape))))

  def joinShape(srcShape: Shape) =
    modSides(lShape(srcShape), rShape(srcShape))

  "Outline ThetaJoin" >> {
    "results from applying combine to branch shapes" >> prop { (srcShape: Shape, jtype: JoinType) =>
      val on = ConstantR[JoinSide](ejsonNull)
      Outline[ThetaJoin].outlineƒ(ThetaJoin(srcShape, l, r, on, jtype, joinFunc)) must_= joinShape(srcShape)
    }
  }

  "Outline EquiJoin" >> {
    "results from applying combine to branch shapes" >> prop { (srcShape: Shape, jtype: JoinType) =>
      Outline[EquiJoin].outlineƒ(EquiJoin(srcShape, l, r, Nil, jtype, joinFunc)) must_= joinShape(srcShape)
    }
  }
}
