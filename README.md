# BigDataProject - Predicting Bitcoin price

hadoop fs -mkdir cs451-projectInput

hadoop fs -put `your data file name` cs451-projectInput

# Stage 1

## Part 1 Data Collection

input `cs451-projectInput/your data file name`

`spark-submit --class ca.uwaterloo.cs451.project.DataProcessing --deploy-mode client \
--num-executors 4 --executor-cores 4 --executor-memory 24G --driver-memory 2g \
target/assignments-1.0.jar \
--input cs451-projectInput/sample.txt \
--output cs451-projectProcessed`

## Part 2 Data Mining

`spark-submit --class ca.uwaterloo.cs451.project.Project --deploy-mode client \
--num-executors 4 --executor-cores 4 --executor-memory 24G --driver-memory 2g \
target/assignments-1.0.jar \
--input cs451-projectProcessed/project \
--output cs451-projectTrained`

## Part 3 Data Evaluation

`spark-submit --class ca.uwaterloo.cs451.project.Evaluation --deploy-mode client \
--num-executors 4 --executor-cores 4 --executor-memory 24G --driver-memory 2g \
target/assignments-1.0.jar \
--input cs451-projectTrained \
--output cs451-projectEvaluated`

# Stage 2

1. We will process the hourly Reddit data, and count how many times bitcoin appears

2. Join the data with price data

3. Use it as a 19-th parameter. (Easily done)


# Notes

We use MLlib in Spark for this project

At first, we find data, apply artifitial neural network to train. 

Then, apply data visualization to see how well the model fits.

# Reference
 
 [Document](https://spark.apache.org/docs/latest/ml-classification-regression.html#multilayer-perceptron-classifier)
 
 [Transform data](https://stackoverflow.com/questions/33844591/prepare-data-for-multilayerperceptronclassifier-in-scala)
 
 [example](https://blog.csdn.net/zjsghww/article/details/84033060)
 
 [Intro](https://medium.com/@Sushil_Kumar/artificial-neural-network-with-spark-mllib-9474570239d8)
 
 [Matrix](https://my.oschina.net/uchihamadara/blog/814017)
















