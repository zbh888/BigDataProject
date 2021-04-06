## Distributed FNN training

Note `GenNum.java` in this repo generates the correct format of input data

Download and try running `java GenNum.java 20` gives 20 lines.

### usage

Run `spark-submit --class ca.uwaterloo.cs451.project.Project target/assignments-1.0.jar --input project/project --output project_result`

Create directory named as `project` under `./src/main/scala/ca/uwaterloo/cs451/project`

Add `Project.scala` to the path `./src/main/scala/ca/uwaterloo/cs451/project`

Replace the `pom.xml` with `pom.xml` in this repo

#### Input structure

Create a directory named as `project` under `bigdata2021w`, it should contain:

projecttest           // Test set -> 10% of the total data set

 └─part-00000
 
projecttrain1         // Train set 1 -> 16% of the total data set

 └─part-00000
 
projecttrain2         // Train set 2 -> 16% of the total data set

 └─part-00000
 
projecttrain3         // Train set 3 -> 16% of the total data set 

 └─part-00000
 
projecttrain4         // Train set 4 -> 16% of the total data set

 └─part-00000
 
projecttrain5         // Train set 5 -> 16% of the total data set

 └─part-00000
 
projectvalidation     // Validation set -> 10% of the total data set

 └─part-00000
 
 #### Output structure
 
 It creates directories `A`, `B`, `C` under `bigdata2021/project_result`
 
 That is the result of three models tested on the testset. The structure of the dataframe is ` ("label", "date", "prediction") : (Int, String, Int)`
 
 Those three models are best performed on validation set among those total five models
 

```
 0 1999-0-0 1
 1 1999-0-1 1
 1 1999-0-2 0
 1 1999-0-3 0
 0 1999-0-4 1
 1 1999-0-5 0
 1 1999-0-6 0
 1 1999-0-7 0
```
 
 A
 
 ├─_SUCCESS
 
 ├─part-00000-b25fff05-4107-4b31-8593-7edc998ccb93-c000.snappy.parquet
 
 └─part-00001-b25fff05-4107-4b31-8593-7edc998ccb93-c000.snappy.parquet
 
B

 ├─_SUCCESS
 
 ├─part-00000-20390d6e-0dc0-447a-a678-7951f3e4d742-c000.snappy.parquet
 
 └─part-00001-20390d6e-0dc0-447a-a678-7951f3e4d742-c000.snappy.parquet
 
C

 ├─_SUCCESS
 
 ├─part-00000-47aa4567-842e-4d7c-a478-cdc2198f695a-c000.snappy.parquet
 
 └─part-00001-47aa4567-842e-4d7c-a478-cdc2198f695a-c000.snappy.parquet
 
 
 # Reference
 
 [Document](https://spark.apache.org/docs/latest/ml-classification-regression.html#multilayer-perceptron-classifier)
 
 [Transform data](https://stackoverflow.com/questions/33844591/prepare-data-for-multilayerperceptronclassifier-in-scala)
 
 [example](https://blog.csdn.net/zjsghww/article/details/84033060)
 
 [Intro](https://medium.com/@Sushil_Kumar/artificial-neural-network-with-spark-mllib-9474570239d8)
 
 [Matrix](https://my.oschina.net/uchihamadara/blog/814017)



