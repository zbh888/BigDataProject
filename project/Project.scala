package ca.uwaterloo.cs451.project

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import org.apache.spark.ml.feature.VectorAssembler
import scala.collection.mutable.Seq

//spark-submit --class ca.uwaterloo.cs451.project.Project target/assignments-1.0.jar --input project/project --output project_result

class ProjectConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object Project {
  val log = Logger.getLogger(getClass().getName())
  def main(argv: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val args = new ProjectConf(argv)
    val input = args.input()
    val output = args.output()

    val assembler = new VectorAssembler()
      .setInputCols(Array("t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11" ,"t12", "t13","t14","t15","t16","t17","t18"))
      .setOutputCol("features")
    val layers = Array[Int](18, 100, 100, 2)
    val trainer = new MultilayerPerceptronClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(88888888)
      .setMaxIter(100)
      .setStepSize(0.05)
    val evaluator = new MulticlassClassificationEvaluator()

    val validation = sc.textFile(input+"validation")
      .map(x => x.substring(1, x.length-1))
      .map(y => y.split(","))
      .map(x => (x(0).toInt,x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,x(9).toDouble,x(10).toDouble,x(11).toDouble,x(12).toDouble,x(13).toDouble,x(14).toDouble,x(15).toDouble,x(16).toDouble,x(17).toDouble,x(18).toDouble,x(19).toDouble))
      .toDF("label", "date", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11" ,"t12", "t13","t14","t15","t16","t17","t18")
    val validation_df = assembler.transform(validation).select("label", "date", "features")

    // Train models
    // Model 1
    val data1 = sc.textFile(input+"train1")
      .map(x => x.substring(1, x.length-1))
      .map(y => y.split(","))
      .map(x => (x(0).toInt,x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,x(9).toDouble,x(10).toDouble,x(11).toDouble,x(12).toDouble,x(13).toDouble,x(14).toDouble,x(15).toDouble,x(16).toDouble,x(17).toDouble,x(18).toDouble,x(19).toDouble))
      .toDF("label", "date", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11" ,"t12", "t13","t14","t15","t16","t17","t18")
    val data1_df = assembler.transform(data1).select("label", "date", "features")
    val model1 = trainer.fit(data1_df)
    val num1 = evaluator.evaluate(model1.transform(validation_df).select("prediction","label"))

    // Model 2
    val data2 = sc.textFile(input+"train2")
      .map(x => x.substring(1, x.length-1))
      .map(y => y.split(","))
      .map(x => (x(0).toInt,x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,x(9).toDouble,x(10).toDouble,x(11).toDouble,x(12).toDouble,x(13).toDouble,x(14).toDouble,x(15).toDouble,x(16).toDouble,x(17).toDouble,x(18).toDouble,x(19).toDouble))
      .toDF("label", "date", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11" ,"t12", "t13","t14","t15","t16","t17","t18")
    val data2_df = assembler.transform(data2).select("label", "date", "features")
    val model2 = trainer.fit(data2_df)
    val num2 = evaluator.evaluate(model2.transform(validation_df).select("prediction","label"))

    // Model 3
    val data3 = sc.textFile(input+"train3")
      .map(x => x.substring(1, x.length-1))
      .map(y => y.split(","))
      .map(x => (x(0).toInt,x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,x(9).toDouble,x(10).toDouble,x(11).toDouble,x(12).toDouble,x(13).toDouble,x(14).toDouble,x(15).toDouble,x(16).toDouble,x(17).toDouble,x(18).toDouble,x(19).toDouble))
      .toDF("label", "date", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11" ,"t12", "t13","t14","t15","t16","t17","t18")
    val data3_df = assembler.transform(data3).select("label", "date", "features")
    val model3 = trainer.fit(data3_df)
    val num3 = evaluator.evaluate(model3.transform(validation_df).select("prediction","label"))

    // Model 4
    val data4 = sc.textFile(input+"train4")
      .map(x => x.substring(1, x.length-1))
      .map(y => y.split(","))
      .map(x => (x(0).toInt,x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,x(9).toDouble,x(10).toDouble,x(11).toDouble,x(12).toDouble,x(13).toDouble,x(14).toDouble,x(15).toDouble,x(16).toDouble,x(17).toDouble,x(18).toDouble,x(19).toDouble))
      .toDF("label", "date", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11" ,"t12", "t13","t14","t15","t16","t17","t18")
    val data4_df = assembler.transform(data4).select("label", "date", "features")
    val model4 = trainer.fit(data4_df)
    val num4 = evaluator.evaluate(model4.transform(validation_df).select("prediction","label"))

    // Model 5
    val data5 = sc.textFile(input+"train5")
      .map(x => x.substring(1, x.length-1))
      .map(y => y.split(","))
      .map(x => (x(0).toInt,x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,x(9).toDouble,x(10).toDouble,x(11).toDouble,x(12).toDouble,x(13).toDouble,x(14).toDouble,x(15).toDouble,x(16).toDouble,x(17).toDouble,x(18).toDouble,x(19).toDouble))
      .toDF("label", "date", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11" ,"t12", "t13","t14","t15","t16","t17","t18")
    val data5_df = assembler.transform(data5).select("label", "date", "features")
    val model5 = trainer.fit(data5_df)
    val num5 = evaluator.evaluate(model5.transform(validation_df).select("prediction","label"))

    // Testing
    val test = sc.textFile(input+"test")
      .map(x => x.substring(1, x.length-1))
      .map(y => y.split(","))
      .map(x => (x(0).toInt,x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,x(9).toDouble,x(10).toDouble,x(11).toDouble,x(12).toDouble,x(13).toDouble,x(14).toDouble,x(15).toDouble,x(16).toDouble,x(17).toDouble,x(18).toDouble,x(19).toDouble))
      .toDF("label", "date", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11" ,"t12", "t13","t14","t15","t16","t17","t18")
    val test_df = assembler.transform(test).select("label", "date", "features")



    val outputDir1 = new Path(output+"/A")
    val outputDir2 = new Path(output+"/B")
    val outputDir3 = new Path(output+"/C")
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir1, true)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir2, true)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir3, true)

    val list = Array((1,num1),(2,num2),(3,num3),(4,num4),(5,num5)).sortBy(x => x._2)
    val a = list(2)._1
    val b = list(3)._1
    val c = list(4)._1

    def WriteToDisk(value : Int, name : String): Unit = {
      if(value == 1) {
        val result = model1.transform(test_df).select("label", "date", "prediction")
        result.write.save(output+"/" + name)
      }
      if(value == 2) {
        val result = model2.transform(test_df).select("label", "date", "prediction")
        result.write.save(output+"/" + name)
      }
      if(value == 3) {
        val result = model3.transform(test_df).select("label", "date", "prediction")
        result.write.save(output+"/" + name)
      }
      if(value == 4) {
        val result = model4.transform(test_df).select("label", "date", "prediction")
        result.write.save(output+"/" + name)
      }
      if(value == 5) {
        val result = model5.transform(test_df).select("label", "date", "prediction")
        result.write.save(output+"/" + name)
      }
    }

    WriteToDisk(a, "A")
    WriteToDisk(b, "B")
    WriteToDisk(c, "C")
  }
}
