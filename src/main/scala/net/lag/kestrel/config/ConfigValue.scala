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

package net.lag.kestrel
package config

/**
 * ConfigValue represents a value that may either be a default or a
 * configuration-specified value. A ConfigValue may be resolved against
 * another value.
 */
sealed abstract class ConfigValue[A](x: A) {
  def isSpecified: Boolean
  def get = x
  def resolve(default: Option[A]): A = {
    default match {
      case Some(_) if isSpecified => get
      case Some(v) => v
      case None => get
    }
  }
}

/*
 * SpecifiedValue is a ConfigValue specified in a configuration.
 */
case class SpecifiedValue[A](x: A) extends ConfigValue[A](x) {
  def isSpecified = true
}

/*
 * Default is a ConfigValue specified as a default.
 */
case class Default[A] private[config] (x: A) extends ConfigValue[A](x) {
  def isSpecified = false
}

/**
 * The ConfigValue companion object provides implicit methods to convert
 * bare objects into SpecifiedValue instances.
 */
object ConfigValue {
  implicit def wrap[A](x: A): ConfigValue[A] = SpecifiedValue(x)
  implicit def wrapToOption[A](x: A): ConfigValue[Option[A]] = SpecifiedValue(Some(x))
  implicit def wrapNone[A](x: None.type): ConfigValue[Option[A]] = SpecifiedValue(None)
}
