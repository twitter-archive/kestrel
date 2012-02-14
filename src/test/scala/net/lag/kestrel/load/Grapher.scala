/*
 * Copyright 2012 Twitter, Inc.
 * Copyright 2012 Robey Pointer <robeypointer@gmail.com>
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

package net.lag.kestrel.load

import com.twitter.ostrich.stats.Histogram

/**
 * Draw a graph of a histogram.
 * The x-axis will be 101 chars wide, to show the edge of each 1% interval from 0 to 100.
 */
class Grapher(histogram: Histogram, scale: Double, height: Int, capY: Option[Int]) {
  def draw() {
    val yValues = new Array[Int](101)
    (0 to 100).foreach { i =>
      yValues(i) = histogram.getPercentile(i / 100.0)
    }
    val yMax = (yValues.max) max (capY getOrElse yValues.max)
    val yQuantum = yMax / height.toDouble
    val graph = (0 until height).map { _ => (0 to 100).map { _ => ' ' }.toArray }.toArray
    (0 to 100).foreach { i =>
      val y = (yValues(i).toDouble / yQuantum).toInt
      (0 until y).foreach { n => graph(n)(i) = '#' }
    }
    val headings = yAxisHeadings(yMax * scale, height)
    (0 until height).foreach { y =>
      println(headings(y) + new String(graph(height - y - 1)))
    }
    println("      +" + ("-" * 101))
  }

  /**
   * Drop a number to its nearest N-digit precision floor (or ceiling)
   */
  private[this] def dropPrecision(number: Double, precision: Int, ceiling: Int = 0): Double = {
    var n = number
    if (n == 0.0) return n
    var pow = 1.0
    while (n >= 1.0) {
      n /= 10.0
      pow *= 10.0
    }
    while (n > 0 && n < 0.1) {
      n *= 10.0
      pow /= 10.0
    }
    pow /= math.pow(10.0, precision)
    ((number / pow).toInt + ceiling) * pow
  }

  private[this] def humanize(num: Double, index: Int = 6): String = {
    val label = "afpnum KMGTPE"
    val n = num.abs
    if (n > 1000.0) {
      humanize(num / 1000.0, index + 1)
    } else if (n < 1.0 && n != 0.0) {
      humanize(num * 1000.0, index - 1)
    } else {
      "%5.3g%s".format(num, label.charAt(index))
    }
  }

  private[this] def yAxisHeadings(yMax: Double, height: Int): Array[String] = {
    val yQuantum = yMax / height.toDouble
    val headings = new Array[String](height)
    headings(0) = humanize(dropPrecision(yQuantum, 3))
    var lastHeading = headings(0)
    (1 until height - 1).foreach { i =>
      val heading = humanize(dropPrecision((i + 1) * yQuantum, 3))
      if (heading != lastHeading && headings(i - 1) == "") {
        headings(i) = heading
        lastHeading = heading
      } else {
        headings(i) = ""
      }
    }
    headings(height - 1) = humanize(yMax)
    headings.map { "%-6s|".format(_) }.reverse
  }
}
