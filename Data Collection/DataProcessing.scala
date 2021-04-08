/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.project

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import java.io._
import scala.collection._
import org.apache.spark.mllib.rdd.RDDFunctions._

class DataProcessingConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle", required = false)
  verify()
}

object DataProcessing {
  def main(argv: Array[String]) {
    val args = new DataProcessingConf(argv)
    val conf = new SparkConf().setAppName("DataProcessing")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val result = textFile.map(p => (p.split(",")(0), p.split(",")(1))).sliding(18, 1)
    .map(p => {(scala.util.Random.nextInt,
                (if (p(0)._2 < p(17)._2) 1 else 0, p(0)._1,
                 p(0)._2, p(1)._2, p(2)._2, p(3)._2, p(4)._2, p(5)._2, p(6)._2, p(7)._2, p(8)._2, p(9)._2,
                 p(10)._2, p(11)._2, p(12)._2, p(13)._2, p(14)._2, p(15)._2, p(16)._2, p(17)._2))
              })
    .sortByKey()
    .map(p => {(scala.util.Random.nextInt(101), p._2)})
    
    var projecttest = result.filter(p => {p._1 <= 10}).map(p => p._2)
    var projecttrain1 = result.filter(p => {p._1 > 10 && p._1 <= 26}).map(p => p._2)
    var projecttrain2 = result.filter(p => {p._1 > 26 && p._1 <= 42}).map(p => p._2)
    var projecttrain3 = result.filter(p => {p._1 > 42 && p._1 <= 58}).map(p => p._2)
    var projecttrain4 = result.filter(p => {p._1 > 58 && p._1 <= 74}).map(p => p._2)
    var projecttrain5 = result.filter(p => {p._1 > 74 && p._1 <= 90}).map(p => p._2)
    var projectvalidation = result.filter(p => {p._1 > 90}).map(p => p._2)
    
    result.map(p => p._2).saveAsTextFile(args.output() + "/result")
    projecttest.saveAsTextFile(args.output() + "/projecttest")
    projecttrain1.saveAsTextFile(args.output() + "/projecttrain1")
    projecttrain2.saveAsTextFile(args.output() + "/projecttrain2")
    projecttrain3.saveAsTextFile(args.output() + "/projecttrain3")
    projecttrain4.saveAsTextFile(args.output() + "/projecttrain4")
    projecttrain5.saveAsTextFile(args.output() + "/projecttrain5")
    projectvalidation.saveAsTextFile(args.output() + "/projectvalidation")
  }
}
