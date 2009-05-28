/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
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

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import org.specs.runner.SpecsFileRunner


object TestRunner extends SpecsFileRunner("src/test/scala/**/*.scala", ".*",
  System.getProperty("system", ".*"), System.getProperty("example", ".*")) {

  System.setProperty("stage", "test")
  if (System.getProperty("debugtrace") == null) {
    Logger.get("").setLevel(Logger.FATAL)
  } else {
    Logger.get("").setLevel(Logger.TRACE)
  }
}
