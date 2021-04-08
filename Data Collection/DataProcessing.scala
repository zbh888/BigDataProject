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

    var size = 0
    val textFile = sc.textFile(args.input())
    val result = textFile.map(p => (p.split(",")(0), p.split(",")(1))).collect().toList.sliding(18, 1)
    .map(p => (if (p(0)._2 < p(17)._2) 1 else 0, p(0)._1,
               p(0)._2, p(1)._2, p(2)._2, p(3)._2, p(4)._2, p(5)._2, p(6)._2, p(7)._2, p(8)._2, p(9)._2,
               p(10)._2, p(11)._2, p(12)._2, p(13)._2, p(14)._2, p(15)._2, p(16)._2, p(17)._2))
    .map(line => { (scala.util.Random.nextInt, line) }).toList.sortBy(_._1)
    .map(p => {size += 1; p._2}).toList
    println(result)
    sc.parallelize(result, 1).saveAsTextFile(args.output() + "/result")
    
    val testSize = (size * 0.1).floor
    val trainSize = (size * 0.16).ceil
    var counter = 0
    var projecttest = List[(Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
    var projecttrain1 = List[(Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
    var projecttrain2 = List[(Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
    var projecttrain3 = List[(Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
    var projecttrain4 = List[(Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
    var projecttrain5 = List[(Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
    var projectvalidation = List[(Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
    result.map(p =>
      {
        if (counter <= testSize) {
          projecttest = projecttest :+ p
        }
        else if (counter <= testSize + trainSize) {
          projecttrain1 = projecttrain1 :+ p
        }
        else if (counter <= testSize + 2 * trainSize) {
          projecttrain2 = projecttrain2 :+ p
        }
        else if (counter <= testSize + 3 * trainSize) {
          projecttrain3 = projecttrain3 :+ p
        }
        else if (counter <= testSize + 4 * trainSize) {
          projecttrain4 = projecttrain4 :+ p
        }
        else if (counter <= testSize + 5 * trainSize) {
          projecttrain5 = projecttrain5 :+ p
        }
        else {
          projectvalidation = projectvalidation :+ p
        }
        counter += 1

        p
      }
    )
    sc.parallelize(projecttest, 1).saveAsTextFile(args.output() + "/projecttest")
    sc.parallelize(projecttrain1, 1).saveAsTextFile(args.output() + "/projecttrain1")
    sc.parallelize(projecttrain2, 1).saveAsTextFile(args.output() + "/projecttrain2")
    sc.parallelize(projecttrain3, 1).saveAsTextFile(args.output() + "/projecttrain3")
    sc.parallelize(projecttrain4, 1).saveAsTextFile(args.output() + "/projecttrain4")
    sc.parallelize(projecttrain5, 1).saveAsTextFile(args.output() + "/projecttrain5")
    sc.parallelize(projectvalidation, 1).saveAsTextFile(args.output() + "/projectvalidation")
  }
}
