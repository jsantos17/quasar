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

package quasar.impl.external

import slamdata.Predef._
import quasar.api.datasource.DatasourceType
import quasar.connector.{HeavyweightDatasourceModule, LightweightDatasourceModule}
import quasar.contrib.fs2.convert
import quasar.impl.DatasourceModule

import java.lang.{
  Class,
  ClassCastException,
  ClassLoader,
  ClassNotFoundException,
  ExceptionInInitializerError,
  IllegalAccessException,
  IllegalArgumentException,
  NoSuchFieldException,
  NullPointerException,
  Object
}
import java.nio.file.{Files, Path}
import java.util.jar.JarFile
import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.effect.{Effect, Sync, Timer}
import cats.syntax.applicativeError._
import fs2.{Segment, Stream}
import fs2.io.file
import jawn.AsyncParser
import jawn.support.argonaut.Parser._
import jawnfs2._
import org.slf4s.Logging
import scalaz.IMap

object ExternalDatasources extends Logging {
  import ExternalConfig._

  val PluginChunkSize = 8192

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def apply[F[_]: Timer](config: ExternalConfig)(
      implicit F: Effect[F], ec: ExecutionContext)
      : Stream[F, IMap[DatasourceType, DatasourceModule]] = {

    val moduleStream = config match {
      case PluginDirectory(directory) =>
        convert.fromJavaStream(F.delay(Files.list(directory)))
          .filter(_.getFileName.toString.endsWith(PluginExtSuffix))
          .flatMap(loadPlugin[F](_))

      case ExplodedDirs(modules) =>
        for {
          exploded <- Stream.emits(modules)

          (cn, cp) = exploded

          classLoader <- ClassPath.classLoader[Stream[F, ?]](ParentCL, cp)

          module <- loadModule[F](cn.value, classLoader)
        } yield module
    }

    moduleStream.fold(IMap.empty[DatasourceType, DatasourceModule])(
      (m, d) => m.insert(d.kind, d))
  }

  ////

  private val ParentCL = this.getClass.getClassLoader

  private val PluginExtSuffix = "." + Plugin.FileExtension

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def loadModule[F[_]: Sync](className: String, classLoader: ClassLoader)
      : Stream[F, DatasourceModule] = {

    def loadModule0(clazz: Class[_])(f: Object => DatasourceModule): Stream[F, DatasourceModule] =
      Sync[Stream[F, ?]]
        .delay(f(clazz.getDeclaredField("MODULE$").get(null)))
        .recoverWith {
          case e @ (_: NoSuchFieldException | _: IllegalAccessException | _: IllegalArgumentException | _: NullPointerException) =>
            warnStream[F](s"Datasource module '$className' does not appear to be a singleton object", Some(e))

          case e: ExceptionInInitializerError =>
            warnStream[F](s"Datasource module '$className' failed to load with exception", Some(e))

          case _: ClassCastException =>
            warnStream[F](s"Datasource module '$className' is not actually a subtype of LightweightDatasourceModule or HeavyweightDatasourceModule", None)
        }

    def loadLightweight(clazz: Class[_]): Stream[F, DatasourceModule] =
      loadModule0(clazz) { o =>
        DatasourceModule.Lightweight(o.asInstanceOf[LightweightDatasourceModule])
      }

    def loadHeavyweight(clazz: Class[_]): Stream[F, DatasourceModule] =
      loadModule0(clazz) { o =>
        DatasourceModule.Heavyweight(o.asInstanceOf[HeavyweightDatasourceModule])
      }

    for {
      clazz <- Sync[Stream[F, ?]].delay(classLoader.loadClass(className)) recoverWith {
        case cnf: ClassNotFoundException =>
          warnStream[F](s"Could not locate class for datasource module '$className'", Some(cnf))
      }

      module <- loadLightweight(clazz) ++ loadHeavyweight(clazz)
    } yield module
  }

  private def loadPlugin[F[_]: Effect: Timer](
      pluginFile: Path)(
      implicit ec: ExecutionContext)
      : Stream[F, DatasourceModule] = {

    val S = Sync[Stream[F, ?]]

    for {
      js <-
        file.readAllAsync[F](pluginFile, PluginChunkSize)
          .chunks
          .map(_.toByteBuffer)
          .parseJson[Json](AsyncParser.SingleValue)

      pluginResult <- Plugin.fromJson[Stream[F, ?]](js)

      plugin <- pluginResult.fold(
        (s, c) => warnStream[F](s"Failed to decode plugin from '$pluginFile': $s ($c)", None),
        _.withAbsolutePaths[Stream[F, ?]](pluginFile.getParent))

      classLoader <- ClassPath.classLoader[Stream[F, ?]](ParentCL, plugin.classPath)

      mainJar = new JarFile(plugin.mainJar.toFile)

      backendModuleAttr <- jarAttribute[F](mainJar, Plugin.ManifestAttributeName)
      versionModuleAttr <- jarAttribute[F](mainJar, Plugin.ManifestVersionName)

      _ <- versionModuleAttr match {
        case None => warnStream[F](s"No '${Plugin.ManifestVersionName}' attribute found in Manifest for '$pluginFile'.", None)
        case Some(version) => infoStream[F](s"Loading $pluginFile with version $version")
      }

      moduleClasses <- backendModuleAttr match {
        case None =>
          warnStream[F](s"No '${Plugin.ManifestAttributeName}' attribute found in Manifest for '$pluginFile'.", None)

        case Some(attr) =>
          S.pure(attr.split(" "))
      }

      moduleClass <- if (moduleClasses.isEmpty)
        warnStream[F](s"No classes defined for '${Plugin.ManifestAttributeName}' attribute in Manifest from '$pluginFile'.", None)
      else
        Stream.segment(Segment.array(moduleClasses)).covary[F]

      mod <- loadModule[F](moduleClass, classLoader)
    } yield mod
  }

  private def jarAttribute[F[_]: Sync](j: JarFile, attr: String): Stream[F, Option[String]] =
    Sync[Stream[F, ?]].delay(Option(j.getManifest.getMainAttributes.getValue(attr)))

  private def warnStream[F[_]: Sync](msg: => String, cause: Option[Throwable]): Stream[F, Nothing] =
    Sync[Stream[F, ?]].delay(cause.fold(log.warn(msg))(log.warn(msg, _))).drain

  private def infoStream[F[_]: Sync](msg: => String): Stream[F, Unit] =
    Sync[Stream[F, ?]].delay(log.info(msg))
}
