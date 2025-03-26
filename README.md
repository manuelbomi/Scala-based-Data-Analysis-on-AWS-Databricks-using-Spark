## Scala-based-Data-Analysis-on-AWS-Databricks-using-Spark
### Spark Advantages
Spark is an open source distributed, parallel-processing framework designed for processing massive data at scale. It addresses many of the limitations of Hadoop. Spark replaces the Hadoop's MapReduce structure with a faster framework that is more suitable for distributed machine learning jobs. Spark also offers more user-friendly APIs (Scala, R, Python, Java) along with its in-memory computing capabilities. Other advantages of Spark include:

##### Fault Torenace: 
Its Resilient Distributed Dataset abstraction enables it to recover quickly if a node fails. Spark also support dataframes, JSON, CSV, Parquet files and a host of other structured and semi-structured file formats. 

#### Lazy Data Evaluation
Spark's lazy data evaluation method enables it to be fast, and it allows for optimized query execution plans

#### Unified Engine for Batch, Streaming and Graph Data Processing
Spark provides at-scale and unified computing framework for a variety of data processing jobs including ML jobs, graph processing, bacth and streaming data analytics.  

## Advantages of Scala
Apache Spark is written in Scala programming language, and it also support Java Virtual Machine (JVM). Scala is written in Java, and it also support JVM. Codes written in Scala compiles into Java bytecodes. This provides excellent computing capabilities, optimization, and ease of intergration with Spark. In addition, Scala's concise and statically typed syntax allows for developers to easily build useful analytic queries with Scala. Its being statically type also enables error discoveries at compile time. 

Scala is in object oriented programming language, and it easily supports inheritance and polymorphism since everything in Scala can be defined as classes and objects. 

In this project, both Scala and Spark are used for data analysis on AWS Databricks (Community Edition). Please check (here:  https://github.com/manuelbomi/Data-Analysis-with-Pyspark-on-AWS-Databricks ) to see how to import data into your AWS Community Edition Databricks. 

To create dataframes using Scala, import data into your Databrick's HDFS or paste data from a source onto a cell in your Databricks. 

Several methods of importing or creating data on you Databrick's cell using Scala are discussed below:

Spark and Scala can read data in various formats
////////////////////// Read CSV data

val csvDF = spark.read .option("header", "true") .csv("path/to/your/file.csv")

//////////////////// Read JSON data

val jsonDF = spark.read.json("path/to/your/file.json")

/////////////////////// Read Parquet data

val parquetDF = spark.read.parquet("path/to/your/file.parquet")

///////////////////////// Write DataFrame to Parquet

evenDF.write.parquet("path/to/output/directory")

////////////////////// // Read data from some online repository
val catalog = "Emmanuel_experiments"

val schema = "Emmanuel_experiments"

val volume = "volume_emmanuel_experiments"

val downloadUrl = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"

val fileName = "rows.csv"

val tableName = "table_name_emm_exp_scala"

val pathVolume = s"/Volumes/$catalog/$schema/$volume"

val pathTable = s"$catalog.$schema"

print(pathVolume) // Show the complete path

print(pathTable) // Show the complete path







![1 create scala dataframe](https://github.com/user-attachments/assets/22847b65-5ef8-40de-b892-b94bdbe6d5d3)


![2 check data schema](https://github.com/user-attachments/assets/fc416e72-116e-49b2-9126-09aa0f23740d)


![3 data can be uploaded into HDFS and read from HDFS](https://github.com/user-attachments/assets/3b982fb8-f87a-4185-bf92-769c173985fe)


![4 Data from HDFS source](https://github.com/user-attachments/assets/1ff9270e-0ec8-4bda-901f-8194ad43739c)


![5 Rename data column](https://github.com/user-attachments/assets/68103821-2fdc-4808-8202-ecbc06f1dc3f)


![6 Apply some filter operations](https://github.com/user-attachments/assets/23f77468-7d9e-4866-ba28-66fed31ade10)


![7 Apply some filter operation](https://github.com/user-attachments/assets/b6cd0e13-76af-4cf8-bdee-ff6f179ebf69)


![8 Aggregation operation](https://github.com/user-attachments/assets/d414bed1-6683-472d-bb10-bf2fbedd0988)


![10 Temoprary view and SQL queries](https://github.com/user-attachments/assets/00d232cc-5e53-4448-9970-c545e5ef3d82)


![11 subset of origila dataframe based on some conditions](https://github.com/user-attachments/assets/23f55f2a-d2a8-4757-9733-753e1d12efd3)


![12 subset of original dataset](https://github.com/user-attachments/assets/9d4a8f1b-c9c1-42e8-bf73-e17a98b8787c)


![13 Spark SelectExpr method](https://github.com/user-attachments/assets/d644e0b7-a6ac-421f-8d20-6bfad6da9449)

![14 Spark Expr method](https://github.com/user-attachments/assets/8deac09c-87f3-4c4a-86b4-3918377435a1)








