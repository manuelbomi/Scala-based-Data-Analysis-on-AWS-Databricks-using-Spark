// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC // Spark and Scala can read data in various formats
// MAGIC
// MAGIC /////////////////////
// MAGIC // Read CSV data
// MAGIC
// MAGIC val csvDF = spark.read
// MAGIC   .option("header", "true")
// MAGIC   .csv("path/to/your/file.csv")
// MAGIC
// MAGIC //////////////////
// MAGIC // Read JSON data
// MAGIC
// MAGIC val jsonDF = spark.read.json("path/to/your/file.json")
// MAGIC
// MAGIC /////////////////////
// MAGIC // Read Parquet data
// MAGIC
// MAGIC val parquetDF = spark.read.parquet("path/to/your/file.parquet")
// MAGIC
// MAGIC ///////////////////////
// MAGIC // Write DataFrame to Parquet
// MAGIC
// MAGIC evenDF.write.parquet("path/to/output/directory")
// MAGIC
// MAGIC
// MAGIC //////////////////////
// MAGIC // Read data from some online repository
// MAGIC
// MAGIC val catalog = "Emmanuel_eperiments"
// MAGIC
// MAGIC val schema = "Emmanuel_eperiments"
// MAGIC
// MAGIC val volume = "volume_emmanuel_experiments"
// MAGIC
// MAGIC val downloadUrl = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
// MAGIC
// MAGIC val fileName = "rows.csv"
// MAGIC
// MAGIC val tableName = "table_name_emm_exp_scala"
// MAGIC
// MAGIC val pathVolume = s"/Volumes/$catalog/$schema/$volume"
// MAGIC
// MAGIC val pathTable = s"$catalog.$schema"
// MAGIC
// MAGIC print(pathVolume) // Show the complete path
// MAGIC
// MAGIC print(pathTable) // Show the complete path

// COMMAND ----------

// MAGIC %md
// MAGIC Below, we show some methods of how data could be read from various sources including tuple, HDFS and S3 bucket for Scala-based Spark analysis

// COMMAND ----------

// Create Scala data from a tuple of data
val data2 = Seq(
  ("Emmanuel", "Oyekanlu", 6111876, "M", 33,  237000,  "manuelbomi@yahoo.com", "Software", 11, 25),
  ("Don", "Coder", 387654, "M", 30,  210000,  "python-Coder@gmail.com", "IT", 12,  14),
  ("Henry", "Charles", 3000127, "M", 42,  210000,  "massie@yahoo.com", "Utilities", 7, 5),
   ("Stephen", "Smith", 9087655, "M", 38,  156000,  "miutss@karen.com", "Front Desk", 5, 3),
  ("Rose", "CarlyWiggle", 5609876, "F", 24,  237000,  "iutyrr@yahoo.com", "HR", 18, 21),
  ("Diddier", "Thomas", 6347652, "M", 53,  237000,  "potyur@yahoo.com", "Software", 19, 33),
  ("Carla", "Fisher", 9871234, "F", 28,  121000,  "tuyinmg@yahoo.com", "Engineering", 3, 34),
  ("Yinka", "Eromonsele", 547863, "F", 29,  99500,  "eromonsele@yahoo.com","Software",12, 18),
  ("Rod", "BiggerStewart", 698328, "M", 54,  76500,  "BiggerS@yahoo.com", "Engineering",12, 21),
  ("Oliver", "Twist", 7652423, "M", 33,  200000, "Twister@yahoo.com", "Utilities", 7, 14),
  ("Moses", "Aaron", 9876543, "M", 23,  186000,  "Moses@cnn.com", "HR", 6, 24),
  ("Molly", "Van Modeller", 6487653, "F", 39,  232000,  "preacher@yahoo.com", "Software", 8, 22),
  ("Barry", "TightFisted", 7864556, "M", 38,  115000,  "boxer@yahoo.com", "IT", 5, 1),
  ("Ken", "Chang", 9845376, "M", 26,  105890,  "bongbonyahoo.com","IT", 10, 20),
  ("Alhaji", "Kareem", 87565234, "M", 44,  65000,  "uytrew@yahoo.com", "IT", 9, 13),
  ("Islam", "Aboubacar", 8719865, "M", 32,  186100,  "westerm@yahoo.com", "IT", 4, 11),
  ("Meghan", "Markle", 7645348, "M", 44,  91000,  "Missyr@yahoo.com", "HR", 2, 23)
  

)

val columns2 = Seq("First_Name", "Last_Name", "ID", "Gender", "Age", "Salary", "email", "Department", "Years_experience", "Donations_Amount")

import spark.implicits._
val df = data2.toDF(columns2: _*)
df.show()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC How to read the same data from an HDFS source after uploading the data into HDFS

// COMMAND ----------


val catalog = "Emmanuel_experiments"
val schema = "Emmanuel_experiments"
val volume = "volume_emmanuel_experiments"
val downloadUrl = "/FileStore/tables/scala_data/scala_data.txt"
val fileName = "rows.txt"
val tableName = "table_name_emm_exp_scala"
val pathVolume = s"/Volumes/$catalog/$schema/$volume"
val pathTable = s"$catalog.$schema"
print(pathVolume) // Show the complete path
print(pathTable) // Show the complete path

// COMMAND ----------

dbutils.fs.cp(downloadUrl, s"$pathVolume/$fileName")

// COMMAND ----------

 val df3 = spark.read
  .option("header", "false")
  .option("inferSchema", "true")
  .option("delimiter", ",")
  .csv(s"$pathVolume/$fileName")

display(df3)

// COMMAND ----------

val col1Renamed = df.withColumnRenamed("First_Name", "First_name") // when modifying a DataFrame in Scala, you must assign it to a new variable
col1Renamed.printSchema()

// COMMAND ----------

col1Renamed.show()

// COMMAND ----------

// count the total number of records in the dataframe
df.count()

// COMMAND ----------

// Display where amoount donated by staff is > 20 and also count the resulting number of records
display(df.filter(df("Donations_amount") > 20))

// COMMAND ----------

// the above can also be accomplished with the code below
display(df.where(df("Donations_amount") > 20))

// COMMAND ----------

//Select columns from the DataFrame and order by frequency
import org.apache.spark.sql.functions.desc
display(df.select("First_Name", "Donations_Amount").orderBy(desc("Donations_amount")))

// COMMAND ----------

import org.apache.spark.sql.functions._ 
val avgSalary = df.groupBy("Department").agg(avg("Salary").as("AvgSalary"))
avgSalary.show()

// COMMAND ----------

// Let us say the management wants to reward employees that makes continuous monthly donations to specified charities by increasing their salaries by some percentage

val transformed_salary= df.withColumn("New_Salary", when($"Donations_Amount" > 20, $"Salary" * 1.2).otherwise($"Salary"))
transformed_salary.show()

// COMMAND ----------

// We can use Scala with SQL queries
// We can also register the DataFrame as a temporary view and use SQL to query the data. This is especially useful for more complex queries.
// Let us check total donations by Department

df.createOrReplaceTempView("all_employees")
val SQLquery = spark.sql("SELECT Department, COUNT(*) AS AllEmployess, SUM(Donations_Amount) AS TotalDonations FROM all_employees GROUP BY Department")
SQLquery.show()

// COMMAND ----------

// Create a subset of the original dataframe 
val subset_of_df_male = df.filter((df("Department") === "Software") && (df("Donations_Amount") > 15) && (df("Gender") === "M")).select("First_Name", "Last_Name","ID", "Salary","Donations_Amount").orderBy(desc("Donations_Amount"))

display(subset_of_df_male)

// COMMAND ----------

// Create another subset of the original dataframe 
val subset_of_df_female = df.filter((df("Department") === "Software") && (df("Donations_Amount") > 15) && (df("Gender") === "F")).select("First_Name", "Last_Name","ID", "Salary","Donations_Amount").orderBy(desc("Donations_Amount"))

display(subset_of_df_female)

// COMMAND ----------

// SQL Queries with Scala
display(df.selectExpr("Donations_Amount", "upper(Last_Name) as Last_Name_Upper"))

// COMMAND ----------

// Use expr() to use SQL syntax for a column
import org.apache.spark.sql.functions.{col, expr}
// Scala requires us to import the col() function as well as the expr() function
display(df.select(col("Donations_Amount"), expr("lower(Last_Name) as Last_Name_Lower")))