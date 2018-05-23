/*
 * gradias: distributed graph algorithms
 * Copyright (C) 2013-2015 Matteo Ceccarello
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.unipd.dei.gradias.util

import java.util.jar.Attributes
import java.util.jar.Attributes.Name._

import scala.collection.JavaConversions

object ManifestKeys {
  val GIT_BRANCH = new Attributes.Name("Git-Branch")
  val GIT_BUILD_DATE = new Attributes.Name("Git-Build-Date")
  val GIT_HEAD_REV = new Attributes.Name("Git-Head-Rev")
}

object ManifestInfo {

  lazy val manifestMap = {
    val resources = getClass().getClassLoader().getResources("META-INF/MANIFEST.MF")
    JavaConversions.enumerationAsScalaIterator(resources).map { r =>
      val mf = new java.util.jar.Manifest(r.openStream())
      JavaConversions.mapAsScalaMap(mf.getMainAttributes())
    }.filter { m =>
      m.get(IMPLEMENTATION_TITLE).map(_ == "gradias").getOrElse(false)
    }.toSeq.headOption
  }

}
