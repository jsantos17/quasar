package ygg.table

import ygg.common._
import scalaz._, Scalaz._
import trans._

trait SamplableTableModule extends TableModule {
  outer =>

  type Table <: SamplableTable

  trait SamplableTable extends TableLike {
    self: Table =>

    type Table >: this.type <: outer.Table
    def sample(sampleSize: Int, specs: Seq[TransSpec1]): M[Seq[Table]]
  }
}

trait SamplableColumnarTableModule extends SamplableTableModule {
  outer: ColumnarTableModule with SliceTransforms =>

  def rng: scala.util.Random = scala.util.Random

  type Table <: ColumnarTable with SamplableTable

  trait SamplableColumnarTable extends SamplableTable {
    self: Table =>

    type Table = outer.Table

    /**
      * A one-pass algorithm for sampling. This runs in time O(H_n*m^2 + n) =
      * O(m^2 lg n + n), so it is not super optimal. Another good option is to
      * try Alissa's approach; keep 2 buffers of size m. Load one up fully,
      * shuffle, then replace, but we aren't 100% sure it is uniform and
      * independent.
      *
      * Of course, the hope is that this will not be used once we get efficient
      * sampling in that runs in O(m lg n) time.
      */
    def sample(sampleSize: Int, specs: Seq[TransSpec1]): M[Seq[Table]] = {
      case class SampleState(rowInserters: Option[RowInserter], length: Int, transform: SliceTransform1[_])

      def build(states: List[SampleState], slices: StreamT[M, Slice]): M[List[Table]] = {
        slices.uncons flatMap {
          case Some((origSlice, tail)) =>
            val nextStates = states map {
              case SampleState(maybePrevInserters, len0, transform) =>
                transform advance origSlice map {
                  case (nextTransform, slice) => {
                    val inserter = maybePrevInserters map { _.withSource(slice) } getOrElse RowInserter(sampleSize, slice)

                    val defined = slice.definedAt

                    @tailrec
                    def loop(i: Int, len: Int): Int =
                      if (i < slice.size) {
                        // `k` is a number between 0 and number of rows we've seen
                        if (!defined(i)) {
                          loop(i + 1, len)
                        } else if (len < sampleSize) {
                          inserter.insert(src = i, dest = len)
                          loop(i + 1, len + 1)
                        } else {
                          val k = rng.nextInt(len + 1)
                          if (k < sampleSize) {
                            inserter.insert(src = i, dest = k)
                          }
                          loop(i + 1, len + 1)
                        }
                      } else len

                    val newLength = loop(0, len0)

                    SampleState(Some(inserter), newLength, nextTransform)
                  }
                }
            }

            Traverse[List].sequence(nextStates) flatMap { build(_, tail) }

          case None =>
            Need {
              states map {
                case SampleState(inserter, length, _) =>
                  val len = length min sampleSize
                  inserter map (_ toSlice len) map { slice =>
                    Table(singleStreamT(slice), ExactSize(len)).paged(yggConfig.maxSliceSize)
                  } getOrElse {
                    Table(emptyStreamT(), ExactSize(0))
                  }
              }
            }
        }
      }

      val transforms = specs map { SliceTransform.composeSliceTransform }
      val states = transforms map { transform =>
        SampleState(None, 0, transform)
      }
      build(states.toList, slices)
    }
  }

  private case class RowInserter(size: Int, slice: Slice, cols: scmMap[ColumnRef, ArrayColumn[_]] = scmMap.empty) {
    import RowInserter._

    def toSlice(maxSize: Int): Slice = Slice(size min maxSize, cols.toMap)

    val ops: Array[ColumnOps] = slice.columns.map(colOpsFor)(collection.breakOut)

    def insert(src: Int, dest: Int) {
      var k = 0
      while (k < ops.length) {
        val col = ops(k)
        col.insert(src, dest)
        k += 1
      }
    }

    def withSource(slice: Slice): RowInserter = RowInserter(size, slice, cols)

    // Creates array columns on demand.
    private def getOrCreateCol(ref: ColumnRef): ArrayColumn[_] = {
      cols.getOrElseUpdate(ref, ref.ctype match {
        case CBoolean             => ArrayBoolColumn.empty()
        case CLong                => ArrayLongColumn.empty(size)
        case CDouble              => ArrayDoubleColumn.empty(size)
        case CNum                 => ArrayNumColumn.empty(size)
        case CString              => ArrayStrColumn.empty(size)
        case CDate                => ArrayDateColumn.empty(size)
        case CPeriod              => ArrayPeriodColumn.empty(size)
        case CArrayType(elemType) => ArrayHomogeneousArrayColumn.empty(size)(elemType)
        case CNull                => MutableNullColumn.empty()
        case CEmptyObject         => MutableEmptyObjectColumn.empty()
        case CEmptyArray          => MutableEmptyArrayColumn.empty()
        case CUndefined           => abort("this shouldn't exist")
      })
    }

    private def colOpsFor: ((ColumnRef, Column)) => ColumnOps = {
      case (ref, col) =>
        (col, getOrCreateCol(ref)) match {
          case (src: BoolColumn, dest: ArrayBoolColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
              def unsafeMove(from: Int, to: Int)          = dest.update(to, dest(from))
            }
          case (src: LongColumn, dest: ArrayLongColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
              def unsafeMove(from: Int, to: Int)          = dest.update(to, dest(from))
            }
          case (src: DoubleColumn, dest: ArrayDoubleColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
              def unsafeMove(from: Int, to: Int)          = dest.update(to, dest(from))
            }
          case (src: NumColumn, dest: ArrayNumColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
              def unsafeMove(from: Int, to: Int)          = dest.update(to, dest(from))
            }
          case (src: StrColumn, dest: ArrayStrColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
              def unsafeMove(from: Int, to: Int)          = dest.update(to, dest(from))
            }
          case (src: DateColumn, dest: ArrayDateColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
              def unsafeMove(from: Int, to: Int)          = dest.update(to, dest(from))
            }
          case (src: NullColumn, dest: MutableNullColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, true)
              def unsafeMove(from: Int, to: Int)          = dest.update(to, true)
            }
          case (src: EmptyObjectColumn, dest: MutableEmptyObjectColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, true)
              def unsafeMove(from: Int, to: Int)          = dest.update(to, true)
            }
          case (src: EmptyArrayColumn, dest: MutableEmptyArrayColumn) =>
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, true)
              def unsafeMove(from: Int, to: Int)          = dest.update(to, true)
            }
          case (src: HomogeneousArrayColumn[a], dest0: ArrayHomogeneousArrayColumn[_]) if src.tpe == dest0.tpe =>
            val dest = dest0.asInstanceOf[ArrayHomogeneousArrayColumn[a]]
            new ColumnOps(src, dest) {
              def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
              def unsafeMove(from: Int, to: Int)          = dest.update(to, dest(from))
            }
          case (src, dest) =>
            abort("Slice lied about column type. Expected %s, but found %s." format (ref.ctype, src.tpe))
        }
    }
  }

  private object RowInserter {
    abstract class ColumnOps(src: Column, dest: ArrayColumn[_]) {
      protected def unsafeInsert(srcRow: Int, destRow: Int): Unit
      protected def unsafeMove(fromRow: Int, toRow: Int): Unit

      final def insert(srcRow: Int, destRow: Int) {
        if (src.isDefinedAt(srcRow)) {
          unsafeInsert(srcRow, destRow)
        } else {
          dest.defined.clear(destRow)
        }
      }

      final def move(from: Int, to: Int) {
        if (dest.isDefinedAt(from)) {
          unsafeMove(from, to)
        } else {
          dest.defined.clear(to)
        }
      }
    }
  }

}
