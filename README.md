# BigDataProject - Predicting Bitcoin price

We will finish the first simple version of this project (Not in real time)

The expectation date to hand in is 2021-4-7

## Part 1 Data Collection

input `cs451-projectInput/sample.txt`

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




















