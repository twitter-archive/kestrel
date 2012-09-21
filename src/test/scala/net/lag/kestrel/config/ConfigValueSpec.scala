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

import org.specs.Specification

class ConfigValueSpec extends Specification {
  "Default" should {
    "return a default value" in {
      Default(1).get mustEqual 1
    }

    "indicate a default value" in {
      Default(1).isSpecified mustEqual false
    }
  }

  "SpecifiedValue" should {
    "return a specified value" in {
      SpecifiedValue(10).get mustEqual 10
    }

    "indicate a specified value" in {
      SpecifiedValue(10).isSpecified mustEqual true
    }
  }

  "ConfigValue" should {
    "resolve against a sequence of ancestors" in {
      "when value is specified" in {
        SpecifiedValue(10).resolve(Some(1)) mustEqual 10
      }

      "when value is not specified" in {
        Default(10).resolve(Some(1)) mustEqual 1
      }

      "when no default is given" in {
        SpecifiedValue(10).resolve(None) mustEqual 10
        Default(10).resolve(None) mustEqual 10
      }
    }
  }
}
