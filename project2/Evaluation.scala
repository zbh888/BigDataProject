package ca.uwaterloo.cs451.project

import org.apache.hadoop.fs.{FileSystem, Path}
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.Seq

// spark-submit --class ca.uwaterloo.cs451.project.Evaluation target/assignments-1.0.jar --input project_result --output project_evaluation

class ConfEvaluation(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object Evaluation {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val conf = new SparkConf().setAppName("Analysis")
    val sc = new SparkContext(conf)
    val args = new ConfEvaluation(argv)

    val input = args.input()
    val output = args.output()
    log.info("Input: " + input)
    log.info("Output: " + output)

    val sparkSession = SparkSession.builder.getOrCreate
    val ADF = sparkSession.read.parquet(input + "/A")
    val BDF = sparkSession.read.parquet(input + "/B")
    val CDF = sparkSession.read.parquet(input + "/C")

    val AB = ADF.join(BDF, Seq("date"))
    val ABC = AB.join(CDF, Seq("date"))
    val newNames = Seq("date", "label", "prediction1", "label2", "prediction2", "label3", "prediction3")
    val Inter = ABC.toDF(newNames: _*)
    // DFrdd contains (label : Int, date : String, prediction : Int)
    val DFrdd = Inter.select("label", "date", "prediction1", "prediction2", "prediction3").rdd
      .map(x => {
        val num = x(2).toString.toFloat + x(3).toString.toFloat + x(4).toString.toFloat
        if(num < 2) {
          (x(0).toString.toFloat.toInt, x(1).toString, 0)
        } else {
          (x(0).toString.toFloat.toInt, x(1).toString, 1)
        }
      })

    val DayAnalysis = DFrdd.map(line => (line._2.substring(0, 10), (line._1, line._3)))
      .groupByKey()
      .map(date => (date._1, date._2.size, date._2))
      .map(date => {
        var list = List[(Int, Int)]()
        val iter = date._3.iterator
        while (iter.hasNext) {
          val pair = iter.next
          if (pair._1 == pair._2) list = list :+ pair
        }
        (date._1, list.size.toFloat / date._2.toFloat)
      })
      .sortByKey()
    val outputDir = new Path(output + "/DayAnalysis")
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    DayAnalysis.saveAsTextFile(output + "/DayAnalysis")
  }
}
