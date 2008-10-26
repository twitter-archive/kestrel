/** Copyright 2008 Twitter, Inc. */
package com.twitter

import org.specs.Specification
import org.specs.runner.SpecsFileRunner
import org.specs.specification.Sut
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
  var specList = new mutable.ListBuffer[Specification]

  // load the specs from class files in the given path.
  private def loadSpecs = {
    for (className <- specificationNames(path, ".*")) {
      createSpecification(className) match {
        case Some(s) => specList += s
        case None => //println("Could not load " + className)
      }
    }

    // filter specs by name, if given.
    System.getProperty("spec") match {
      case null =>
      case filterString =>
        val re = Pattern.compile(filterString)
        for (spec <- specList) {
          spec.suts = spec.suts filter { sut => re.matcher(sut.description).find }
        }
    }

    // also filter examples by name, if given.
    System.getProperty("example") match {
      case null =>
      case filterString =>
        val re = Pattern.compile(filterString)
        for (spec <- specList; sut <- spec.suts) {
          val examples = sut.examples filter { example => re.matcher(example.description).find }
          sut.examples.clear
          sut.examples ++= examples
        }
    }

    // remove any now-empty specs so we don't clutter the output.
    for (spec <- specList) {
      spec.suts = spec.suts filter { sut => sut.examples.length > 0 }
    }
    for (empty <- specList filter { spec => spec.suts.length == 0 }) {
      specList -= empty
    }
  }

  override def reportSpecs = {
    loadSpecs

    // this specification is added for better reporting
    object totalSpecification extends Specification {
      new java.io.File(path).getAbsolutePath isSpecifiedBy(specList: _*)
    }
    super.report(List(totalSpecification))
  }
}
