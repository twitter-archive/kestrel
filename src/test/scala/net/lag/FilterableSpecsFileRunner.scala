/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@lag.net>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag

import org.specs.Specification
import org.specs.runner.SpecsFileRunner
import org.specs.specification.Sus
import scala.collection.mutable
import java.util.regex.Pattern


/**
 * Runs specs against a console, finding all Specification objects in a given path, and
 * optionally filtering the executed Specifications/examples against a regex provided via
 * system properties.
 *
 * The <code>spec</code> property will be used to filter Specifications, and
 * <code>example</code> will filter examples within those Specifications. Because of
 * limitations in the current layout of the specs library, examples can only be filtered at
 * the top level (nested examples are not filtered).
 *
 * Substring regex matching is used, so the simple case of <code>-Dspec=Timeline</code> (a
 * substring with no special regex characters) should do what you expect.
 */
class FilterableSpecsFileRunner(path: String) extends SpecsFileRunner(path, ".*") {
  override lazy val specs = loadSpecs

  def findSpecNames(path: String, pattern: String): List[String] = {
    val result = new mutable.ListBuffer[String]
    val p = Pattern.compile("\\s*object\\s*(" + pattern + ")\\s*extends\\s*.*Spec.*\\s*\\{")
    for (filePath <- filePaths(path); if filePath.endsWith(".scala")) {
      val m = p.matcher(readFile(filePath))
      while (m.find) {
        result += ((packageName(filePath).map(_ + ".").getOrElse("") + m.group(1).trim) + "$")
      }
    }
    result.toList
  }

  // load the specs from class files in the given path.
  private def loadSpecs = {
    var specList = new mutable.ListBuffer[Specification]

    for (className <- findSpecNames(path, ".*")) {
      try {
        getClass.getClassLoader.loadClass(className).newInstance match {
          case spec: Specification => specList += spec
          case _ =>
        }
      } catch {
        case _ => // ignore and skip
      }
    }

    // filter specs by name, if given.
    System.getProperty("spec") match {
      case null =>
      case filterString =>
        val re = Pattern.compile(filterString)
        for (spec <- specList) {
          spec.systems = spec.systems filter { s => re.matcher(s.description).find }
        }
    }

    // also filter examples by name, if given.
    System.getProperty("example") match {
      case null =>
      case filterString =>
        val re = Pattern.compile(filterString)
        for (spec <- specList; system <- spec.systems) {
          val examples = system.examples filter { example => re.matcher(example.description).find }
          system.examples.clear
          system.examples ++= examples
        }
    }

    // remove any now-empty specs so we don't clutter the output.
    for (spec <- specList) {
      spec.systems = spec.systems filter { s => s.examples.length > 0 }
    }
    for (empty <- specList filter { spec => spec.systems.length == 0 }) {
      specList -= empty
    }
    specList.toList
  }
}
