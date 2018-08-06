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

package quasar.precog

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalacheck.Parameters
import org.scalacheck._

object BitSetSpec extends Specification with ScalaCheck {

  implicit val params = Parameters(
    minSize = 100,
    maxSize = 1000,
    workers = Runtime.getRuntime.availableProcessors,
    minTestsOk = 10000)

  implicit val arbBitSet: Arbitrary[BitSet] = {
    Arbitrary(Arbitrary.arbitrary[scala.collection.BitSet] map { bools =>
      val bs = new BitSet(bools.size)
      for (i <- 0 until bools.size) {
        bs(i) = bools(i)
      }
      bs
    })
  }

  "bitset (java)" should {
    "sparsenByMod" in prop { (bs: BitSet, offset0: Int, mod0: Int) =>
      (mod0 > Int.MinValue && offset0 > Int.MinValue) ==> {
        val mod = math.abs(mod0) % 20    // don't let scalacheck go crazy

        (mod > 0) ==> {
          val offset = math.abs(offset0) % mod

          val sparsened = bs.sparsenByMod(offset, mod)

          (sparsened.length << 6) must beEqualTo((bs.length << 6) * mod)
          
          (0 until (sparsened.length << 6)) forall { i: Int =>
            if (i % mod == offset)
              bs(i / mod) must beEqualTo(sparsened(i)).setMessage(s"bs(${i / mod}) != sparsened($i)")
            else
              sparsened(i) must beEqualTo(false).setMessage(s"sparsened($i) != false")
          }
        }
      }
    }

    "setByMod" in prop { (bs: BitSet, mod0: Int) =>
      (mod0 > Int.MinValue) ==> {
        val mod = math.abs(mod0) % 100    // don't let scalacheck go crazy

        (mod > 0) ==> {
          val modded = bs.copy
          modded.setByMod(mod)

          val bound = bs.length << 6

          (0 until bound) must contain { i: Int =>
            if (bs(i))
              modded(i) must beTrue.setMessage(s"modded($i) was false while bs($i) is true")
            else
              ok
          }.forall

          (0 until (bound / mod) by mod) must contain { i: Int =>
            (0 until mod) must contain { j: Int =>
              if (i + j < bound)    // mod might not divide bound
                modded(i + j) must beTrue.setMessage(s"modded(${i + j}) was false")
              else
                ko
            }
          }.forall
        }
      }
    }

    "setByMod avoid positive wrap-around masking issues" in {
      val bs = new BitSet(128)    // ensure we have two longs

      // 64 % 10 = 4, so we should wrap-around mid-mask
      bs.setByMod(10)

      // sanity check on first mod
      (0 until 10) must contain { i: Int =>
        bs(i) must beTrue
      }

      (60 until 70) must contain { i: Int =>
        bs(i) must beTrue
      }
    }

    // note that these negative tests assume that we're setting the mod zero bit
    // that's an implementation detail and technically not forced by the contract
    "setByMod avoid negative wrap-around masking issues" in {
      val bs = new BitSet(128)    // ensure we have two longs

      bs.set(64)    // first bit in the second long

      // 64 % 10 = 4, so we should wrap-around mid-mask
      bs.setByMod(10)

      bs(60) must beFalse
      bs(61) must beFalse
      bs(62) must beFalse
      bs(63) must beFalse
      bs(64) must beTrue
      bs(65) must beFalse
      bs(66) must beFalse
      bs(67) must beFalse
      bs(68) must beFalse
      bs(69) must beFalse
    }

    "setByMod avoid mask overlap on mod > 32" in {
      val bs = new BitSet(448)    // ensure we have seven longs

      bs.set(415)
      bs.set(416)

      // 64 % 35 = 29, and 64 * 6 = 384, and 384 % 35 = 0
      bs.setByMod(35)

      bs(35) must beTrue

      bs(350) must beTrue
      (351 to 384).map(bs(_)) must contain(beFalse).forall
    }

    "setByMod works with mod 3" in {
      val bs = new BitSet(128)    // ensure we have seven longs

      // 64 % 35 = 29, and 64 * 6 = 384, and 384 % 35 = 0
      bs.setByMod(3)

      bs(3) must beTrue

      bs(63) must beTrue
    }
  }
}
