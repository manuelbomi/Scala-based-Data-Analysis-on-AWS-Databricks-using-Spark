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

Developers can also create data in form of a tuple or JSON list. In this project, we shall explore how to use Scala, Spark and SQL to perform some data analysis on such created data. The code is shown below. Interested developer can also download the full code form this repository. 

##### Create Scala data from a tuple of data

val data2 = Seq(

("Emmanuel", "Oyekanlu", 6111876, "M", 33, 237000, "manuelbomi@yahoo.com", "Software", 11, 25),

("Don", "Coder", 387654, "M", 30, 210000, "python-Coder@gmail.com", "IT", 12, 14),

("Henry", "Charles", 3000127, "M", 42, 210000, "massie@yahoo.com", "Utilities", 7, 5),

("Stephen", "Smith", 9087655, "M", 38, 156000, "miutss@karen.com", "Front Desk", 5, 3),

("Rose", "CarlyWiggle", 5609876, "F", 24, 237000, "iutyrr@yahoo.com", "HR", 18, 21),

("Diddier", "Thomas", 6347652, "M", 53, 237000, "potyur@yahoo.com", "Software", 19, 33),

("Carla", "Fisher", 9871234, "F", 28, 121000, "tuyinmg@yahoo.com", "Engineering", 3, 34),

("Yinka", "Eromonsele", 547863, "F", 29, 99500, "eromonsele@yahoo.com","Software",12, 18),

("Rod", "BiggerStewart", 698328, "M", 54, 76500, "BiggerS@yahoo.com", "Engineering",12, 21),

("Oliver", "Twist", 7652423, "M", 33, 200000, "Twister@yahoo.com", "Utilities", 7, 14),

("Moses", "Aaron", 9876543, "M", 23, 186000, "Moses@cnn.com", "HR", 6, 24),

("Molly", "Van Modeller", 6487653, "F", 39, 232000, "preacher@yahoo.com", "Software", 8, 22),

("Barry", "TightFisted", 7864556, "M", 38, 115000, "boxer@yahoo.com", "IT", 5, 1),

("Ken", "Chang", 9845376, "M", 26, 105890, "bongbonyahoo.com","IT", 10, 20),

("Alhaji", "Kareem", 87565234, "M", 44, 65000, "uytrew@yahoo.com", "IT", 9, 13),

("Islam", "Aboubacar", 8719865, "M", 32, 186100, "westerm@yahoo.com", "IT", 4, 11),

("Meghan", "Markle", 7645348, "M", 44, 91000, "Missyr@yahoo.com", "HR", 2, 23)

)

val columns2 = Seq("First_Name", "Last_Name", "ID", "Gender", "Age", "Salary", "email", "Department", "Years_experience", "Donations_Amount")

import spark.implicits._

val df = data2.toDF(columns2: _*)

df.show()







![1 create scala dataframe](https://github.com/user-attachments/assets/22847b65-5ef8-40de-b892-b94bdbe6d5d3)

##### Check data schema

![2 check data schema](https://github.com/user-attachments/assets/fc416e72-116e-49b2-9126-09aa0f23740d)

##### Data could also be read from Databrick's HDFS after users upload the data into HDFS

![3 data can be uploaded into HDFS and read from HDFS](https://github.com/user-attachments/assets/3b982fb8-f87a-4185-bf92-769c173985fe)


![4 Data from HDFS source](https://github.com/user-attachments/assets/1ff9270e-0ec8-4bda-901f-8194ad43739c)

##### Rename data column from the original dataframe (i.e., df)

When modifying a DataFrame in Scala, you must assign it to a new variable

val col1Renamed = df.withColumnRenamed("First_Name", "First_name") 

col1Renamed.printSchema()

![5 Rename data column](https://github.com/user-attachments/assets/68103821-2fdc-4808-8202-ecbc06f1dc3f)

##### Apply some filter operations
count the total number of records in the dataframe

df.count()

##### Display where amoount donated by staff is > 20 and also count the resulting number of records
display(df.filter(df("Donations_amount") > 20))

alternately, the above can also be accomplished with the code below

display(df.where(df("Donations_amount") > 20))

![6 Apply some filter operations](https://github.com/user-attachments/assets/23f77468-7d9e-4866-ba28-66fed31ade10)

##### Select columns from the DataFrame and order by frequency
import org.apache.spark.sql.functions.desc

display(df.select("First_Name", "Donations_Amount").orderBy(desc("Donations_amount")))

![7 Apply some filter operation](https://github.com/user-attachments/assets/b6cd0e13-76af-4cf8-bdee-ff6f179ebf69)

##### Perform an aggregation operation
import org.apache.spark.sql.functions._ 

val avgSalary = df.groupBy("Department").agg(avg("Salary").as("AvgSalary"))

avgSalary.show()
![8 Aggregation operation](https://github.com/user-attachments/assets/d414bed1-6683-472d-bb10-bf2fbedd0988)


![10 Temoprary view and SQL queries](https://github.com/user-attachments/assets/00d232cc-5e53-4448-9970-c545e5ef3d82)


![11 subset of origila dataframe based on some conditions](https://github.com/user-attachments/assets/23f55f2a-d2a8-4757-9733-753e1d12efd3)


![12 subset of original dataset](https://github.com/user-attachments/assets/9d4a8f1b-c9c1-42e8-bf73-e17a98b8787c)


![13 Spark SelectExpr method](https://github.com/user-attachments/assets/d644e0b7-a6ac-421f-8d20-6bfad6da9449)

![14 Spark Expr method](https://github.com/user-attachments/assets/8deac09c-87f3-4c4a-86b4-3918377435a1)








