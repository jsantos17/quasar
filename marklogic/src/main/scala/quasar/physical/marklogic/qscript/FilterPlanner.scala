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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.fp.free._
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.xcc.Xcc
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._
import quasar.qscript.{MapFuncsCore => MFCore, MFC => _, _}
import quasar.{RenderTree, NonTerminal, Terminal}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.patterns._
import matryoshka.implicits._
import pathy._, Path._


import scalaz._, Scalaz._

private[qscript] final class FilterPlanner[
  F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
  FMT: SearchOptions,
  T[_[_]]: BirecursiveT: ShowT
](implicit SP: StructuralPlanner[F, FMT]) {

  case class ProjectPath[A](src: A, path: ADir)

  object ProjectPath extends ProjectPathInstances

  sealed abstract class ProjectPathInstances {
    implicit def functorProjectPath: Functor[ProjectPath] =
      new Functor[ProjectPath] {
        def map[A, B](fa: ProjectPath[A])(f: A => B) = ProjectPath(f(fa.src), fa.path)
      }

    implicit def delayRenderTree[A]: Delay[RenderTree, ProjectPath] =
      Delay.fromNT(λ[RenderTree ~> (RenderTree ∘ ProjectPath)#λ](rt =>
        RenderTree.make(pp =>
          NonTerminal(List("ProjectPath"), none,
            List(rt.render(pp.src), Terminal(List("Path"), prettyPrint(pp.path).some))))))
  }

  type CoMapFunc[T[_[_]], A]     = CoEnv[Hole, MapFunc[T, ?], A]
  type CoPathMapFunc[T[_[_]], A] = CoEnv[Hole, PathMapFunc[T, ?], A]
  type FreePathMap[T[_[_]]]      = Free[PathMapFunc[T, ?], Hole]
  type PathMapFunc[T[_[_]], A]   = Coproduct[ProjectPath, MapFunc[T, ?], A]

  def plan[Q](src: Search[Q] \/ XQuery, f: FreeMap[T])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Search[Q] \/ XQuery] = src match {
    case \/-(src) => xqueryFilter(src, f) map (_.right)
    case -\/(src) => planPredicate[T, Q](f)
        .fold(fallbackFilter(src, f) map (_.right[Search[Q]]))(searchFilter(src, f))
  }

  private def searchFilter[Q](src: Search[Q], f: FreeMap[T])(q: Q)(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Search[Q] \/ XQuery] =
    queryIsValid[F, Q, T[EJson], FMT](planAsSearch(src, f, q).query)
      .ifM(planAsSearch(src, f, q).left[XQuery].point[F], fallbackFilter(src, f) map (_.right[Search[Q]]))

  private def fallbackFilter[Q](src: Search[Q], f: FreeMap[T])(
    implicit Q: Recursive.Aux[Q, Query[T[EJson], ?]]
  ): F[XQuery] =
    (interpretSearch[Q](src) >>= (xqueryFilter(_: XQuery, f)))

  private def planAsSearch[Q](src: Search[Q], f: FreeMap[T], q: Q)(
    implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
  ): Search[Q] =
    Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src)

  private def xqueryFilter(src: XQuery, fm: FreeMap[T]): F[XQuery] =
    for {
      x   <- freshName[F]
      p   <- mapFuncXQuery[T, F, FMT](fm, ~x) map (xs.boolean)
    } yield src match {
      case IterativeFlwor(bindings, filter, order, isStable, result) =>
        XQuery.Flwor(
          bindings :::> IList(BindingClause.let_(x := result)),
          Some(filter.fold(p)(_ and p)),
          order,
          isStable,
          ~x)

      case _ =>
        for_(x in src) where_ p return_ ~x
    }

  private def interpretSearch[Q](s: Search[Q])(implicit Q: Recursive.Aux[Q, Query[T[EJson], ?]]): F[XQuery] =
    Search.plan[F, Q, T[EJson], FMT](s, EJsonPlanner.plan[T[EJson], F, FMT])

  private def planPredicate[T[_[_]]: RecursiveT, Q](fm: FreeMap[T])(
    implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
  ): Option[Q] = {
    elideGuards(foldProjectField(fm)) match {
      case Embed(CoEnv(\/-(MFC(MFCore.Eq(Embed(CoEnv(\/-(PathProject(pp)))), Embed(CoEnv(\/-(MFC(MFCore.Constant(v)))))))))) =>
        Query.PathRange[T[EJson], Q](IList(prettyPrint(pp.path).dropRight(1)), ComparisonOp.EQ, IList(v)).embed.some
    }
  }

  private object PathProject {
    def unapply[T[_[_]], A](pr: PathMapFunc[T, A]): Option[ProjectPath[A]] =
      Inject[ProjectPath, PathMapFunc[T, ?]].prj(pr)
  }

  private object MFC {
    def unapply[T[_[_]], A](pr: PathMapFunc[T, A]): Option[MapFuncCore[T, A]] =
      Inject[MapFuncCore[T, ?], PathMapFunc[T, ?]].prj(pr)
  }

  private object MF {
    def unapply[T[_[_]], A](pr: MapFunc[T, A]): Option[MapFuncCore[T, A]] =
      Inject[MapFuncCore[T, ?], MapFunc[T, ?]].prj(pr)
  }

  /* Discards nested projection guards. The existence of a path range index a/b/c
   * guarantees that the nested projection a/b/c is valid. */
  private def elideGuards[T[_[_]]: RecursiveT](fpm: FreePathMap[T]): FreePathMap[T] = {
    val alg: CoPathMapFunc[T, FreePathMap[T]] => CoPathMapFunc[T, FreePathMap[T]] = totally {
      case CoEnv(\/-(MFC(MFCore.Guard(Embed(CoEnv(\/-(PathProject(_)))), _, cont, _)))) =>
        CoEnv(cont.resume.swap)
    }

    fpm.transCata[FreePathMap[T]](alg)
  }

  private def foldProjectField[T[_[_]]: RecursiveT](fm: FreeMap[T]): FreePathMap[T] = {
    val alg: AlgebraicGTransform[(FreeMap[T], ?), FreePathMap[T], CoMapFunc[T, ?], CoPathMapFunc[T, ?]] = {
      case CoEnv(\/-(MF(MFCore.ProjectField((_, Embed(CoEnv(\/-(PathProject(path))))), (MFCore.StrLit(field), _))))) => {
        val dir0 = path.path </> dir(field)
        val pp   = ProjectPath(path.src, dir0)

        CoEnv(Inject[ProjectPath, PathMapFunc[T, ?]].inj(pp).right)
      }
      case CoEnv(\/-(MF(MFCore.ProjectField((Embed(CoEnv(\/-(src))), _), (MFCore.StrLit(field), _))))) => {
        val dir0 = rootDir[Sandboxed] </> dir(field)
        val desc = Free.roll(src).mapSuspension(injectNT[MapFunc[T, ?], PathMapFunc[T, ?]])
        val pp   = ProjectPath(desc, dir0)

        CoEnv(Inject[ProjectPath, PathMapFunc[T, ?]].inj(pp).right)
      }
      case CoEnv(\/-(other)) =>
        CoEnv(Inject[MapFunc[T, ?], PathMapFunc[T, ?]].inj(other.map(_._2)).right)
      case CoEnv(-\/(h)) => CoEnv(h.left)
    }

    fm.transPara[FreePathMap[T]](alg)
  }

}
