## Distributed FNN training

### usage

*spark-submit --class ca.uwaterloo.cs451.project.Project target/assignments-1.0.jar --input project/project --output project_result*

Create directory named as `project` under `./src/main/scala/ca/uwaterloo/cs451/project`

Add `Project.scala` to the path `./src/main/scala/ca/uwaterloo/cs451/project`

Replace the `pom.xml` with `pom.xml` in this repo

#### Input structure

Create a directory named as `project` under `bigdata2021w`, it should contain:

projecttest // Test set

 └─part-00000
 
projecttrain1 // Train set 1

 └─part-00000
 
projecttrain2 // Train set 2

 └─part-00000
 
projecttrain3 // Train set 3

 └─part-00000
 
projecttrain4 // Train set 4

 └─part-00000
 
projecttrain5 // Train set 5

 └─part-00000
 
projectvalidation // Validation set

 └─part-00000
 
 #### Output structure
 
 It creates directories `A`, `B`, `C` under `bigdata2021/project_result`
 
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



