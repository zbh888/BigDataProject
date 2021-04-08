package ca.uwaterloo.cs451.project

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import java.io.File

class ConfEvaluation(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object Evaluation {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfEvaluation(argv)

    val input = args.input()
    val output = args.output()
    log.info("Input: " + input)
    log.info("Output: " + output)

    val sparkSession = SparkSession.builder.getOrCreate

    val Adir = new File(input + "/A")
    val Bdir = new File(input + "/B")
    val Cdir = new File(input + "/C")
    
    val Afiles = Adir.listFiles.filter(_.isFile).filter(_.toString.contains("part")).filter(!_.toString.contains("crc")).toList
    val Bfiles = Bdir.listFiles.filter(_.isFile).filter(_.toString.contains("part")).filter(!_.toString.contains("crc")).toList
    val Cfiles = Cdir.listFiles.filter(_.isFile).filter(_.toString.contains("part")).filter(!_.toString.contains("crc")).toList

    val ADF = sparkSession.read.parquet(Afiles(0).toString)
      .union(sparkSession.read.parquet(Afiles(1).toString))

    val BDF = sparkSession.read.parquet(Bfiles(0).toString)
      .union(sparkSession.read.parquet(Bfiles(1).toString))
    
    val CDF = sparkSession.read.parquet(Cfiles(0).toString)
      .union(sparkSession.read.parquet(Cfiles(1).toString))

    val ARDD = ADF.rdd
    val BRDD = BDF.rdd
    val CRDD = CDF.rdd

    val A = ARDD.map(line => (line(1).toString.substring(0, 10), (line(0).toString.toInt, line(2).toString.toFloat.toInt)))
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
      .sortByKey(numPartitions=1)
    A.saveAsTextFile(output + "/A")
    //val A2 = sparkSession.createDataFrame(A).toDF("date", "accuracy")
    //A2.write.parquet(output + "/A")

    val B = BRDD.map(line => (line(1).toString.substring(0, 10), (line(0).toString.toInt, line(2).toString.toFloat.toInt)))
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
      .sortByKey(numPartitions=1)
    B.saveAsTextFile(output + "/B")

    val C = CRDD.map(line => (line(1).toString.substring(0, 10), (line(0).toString.toInt, line(2).toString.toFloat.toInt)))
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
      .sortByKey(numPartitions=1)
    C.saveAsTextFile(output + "/C")

  }
}

