# BigDataProject - Predicting Bitcoin price

We will finish the first simple version of this project (Not in real time)

The expectation date to hand in is 2021-4-7

## Part 1 Data Collection

### 1. Download the historical data and save it in HDFS.

Process the data, and it would seem like this: (for data.txt)
```
(time1, 13$)
(time2, 16$)
(time3, 19$)
(time4, 5$)
(time5, 7$) 
(...,  ...)
```

This upload the file

@datasci:\~/Project$ hadoop fs -mkdir cs451-project
@datasci:\~/Project$ hadoop fs -put sample.txt cs451-project

### 2. Processing the data - A

We can ignore keeping the time for our first version. So the data would look like this (No `NA`)

```
(13$)
(16$)
(19$)
(5$)
(7$) 
(...)
```
The order must be correct

### 3. Processing the data - B

Implement a class called `DataProcessing` in scala

```
spark-submit --class ca.uwaterloo.cs451.DataCollecting --deploy-mode client 
--num-executors 4 --executor-cores 4 --executor-memory 24G --driver-memory 2g \
target/assignments-1.0.jar \
--input data.txt \
--output cs451-bigdatateach-ProcessedData \
--parameter 40 \
```

The `parameter` in the command means we pick 40 continuous prices as a group

And the last element must be one of 1(up), 0(down). We use supervised learning first, since unsupervised learning could be unpredictable. 

The resulting data format should look like this

```
(13$, 3$, 6$, 9$, ..., 11$, 0)   //41 in total
(16$ ...                    ... 1)   //41 in total
(19$ ...                    ... 1)   //41 in total
(5$  ...                    ... 1)   //41 in total
(7$  ...                    ... 0)   //41 in total
(...)
```
### 4. Separate the data

Divide the data to `Train`, `Test` data set

Save as `cs451-bigdatateach-ProcessedDataTrain`
and `cs451-bigdatateach-ProcessedDataTest` respectively.

## Part 2 Data Mining

Using the appropriate method to build machine learning model.

My idea is to use a simple neural network using built in library pytorch.

We might consider using PySpark since it is installed on DataSci.

### Train:

Implement a class called `DataMiningTrain` in scala
```
spark-submit --class ca.uwaterloo.cs451.DataMiningTrain --deploy-mode client 
--num-executors 4 --executor-cores 4 --executor-memory 24G --driver-memory 2g \
target/assignments-1.0.jar \
--input cs451-bigdatateach-ProcessedTrain \
--output cs451-bigdatateach-DataMiningModel \
--parameter 40 \
--numModel 10 
```

Take `parameter` as an input to derive models with amount equals `numModel`.

### Test:

Implement a class called `DataMiningTest` in scala
```
spark-submit --class ca.uwaterloo.cs451.DataMiningTest --deploy-mode client 
--num-executors 4 --executor-cores 4 --executor-memory 24G --driver-memory 2g \
target/assignments-1.0.jar \
--input cs451-bigdatateach-ProcessedTest \
--output cs451-bigdatateach-DataPrediction \
--parameter 40 \
--numModel 10 
```

## Part 3 Data Evaluation

Write a script or program to evaluate the accuracy of the result.
If possible, write a program that computes the accuracy for different price level.

Like:
```
40 data as a group, accuracy for those price around 100$ is 60%, (we have 60 data in such level of price)
                    accuracy for those price around 400$ is 30%, (we have 25 data in such level of price)
```

Try changing values of parameters of the all programs to find some better value for increasing the accuracy.

At last, make the analysis.





















