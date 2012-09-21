/*
 * Copyright 2012 Twitter, Inc.
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

package net.lag.kestrel.config

import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import org.specs.Specification

class QueueConfigSpec extends Specification {
  "QueueBuilder" should {
    val sampleConfig =
      QueueConfig(1, 1.byte, 1.byte, None, 1.byte, 1.byte, 1.byte, false, false, 0.seconds, None, 1, false, None)

    val unconfiguredBuilder = new QueueBuilder()
    val configSettings = unconfiguredBuilder.getClass.getMethods.filter {
      _.getReturnType() == classOf[ConfigValue[_]]
    }.map { m => (m.getName, m) }

    "have the correct number of settings" in {
      // e.g. one per field in QueueConfig
      configSettings.size mustEqual sampleConfig.productArity
    }

    // Guarantees that the implicit functions which convert an object to a SpecifiedValue
    // are not invoked by the QueueBuilder itself (only configs should use the implicit
    // conversions).
    configSettings.foreach { case (name, method) =>
      "unconfigured QueueBuilder returns default value for %s".format(name) in {
        val value = method.invoke(unconfiguredBuilder)
        (value.getClass == classOf[Default[_]]) mustEqual true
      }
    }

    "implicitly convert overridden values directly to SpecifiedValue[_] instances" in {
      val builder = new QueueBuilder() {
        maxItems = 100
        maxSize = 1000.bytes
        maxAge = Some(1.day)
      }

      builder.maxItems mustEqual SpecifiedValue(100)
      builder.maxSize mustEqual SpecifiedValue(1000.bytes)
      builder.maxAge mustEqual SpecifiedValue(Some(1.day))
    }

    "implicitly convert overridden values to SpecifiedValue[Option[_]] instances" in {
      val builder = new QueueBuilder() {
        maxAge = 1.day
        maxQueueAge = None
      }

      builder.maxAge mustEqual SpecifiedValue(Some(1.day))
      builder.maxQueueAge mustEqual SpecifiedValue(None)
    }
  }

}
