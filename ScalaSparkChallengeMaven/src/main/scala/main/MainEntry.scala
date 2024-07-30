package main

import session.ScalaSparkSession
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MainEntry {
  
  def main(args: Array[String]): Unit = {
    
    
    val spark = ScalaSparkSession.sparkSession()
    
    import spark.implicits._
    
    val csvPath_ur = ".\\csv\\googleplaystore_user_reviews.csv"
    val df_gps_userReviews = spark.read.format("csv").option("header",true).option("inferSchema", "true").load(csvPath_ur)
    val csvPath_pl = ".\\csv\\googleplaystore.csv"
    val df_googleplaystore = spark.read.format("csv").option("header",true).option("inferSchema", "true").load(csvPath_pl)
    
    //ex1
    
    val df1_1 = df_gps_userReviews.select(
    col("App").cast(StringType).as("App"),  // Change type and rename
    col("Sentiment_Polarity").cast(DoubleType).as("Sentiment_Polarity")
    )
    
    val df1_2 = df1_1.na.fill(0, Seq("Sentiment_Polarity"))  // Fill NA with 0 for the specific column
    
   	// Group by App name and calculate the average review score
   	val df1 = df1_2
  	.groupBy("App")
  	.agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    df1.show()
    
    
    //ex2
    
    val filtered_df_googleplaystore: DataFrame = df_googleplaystore.filter("Rating >= 40.0").orderBy(desc("Rating"))

    filtered_df_googleplaystore.write
    .option("delimiter", "§")
    .option("header", "true")  // Include header in the output CSV
    .csv(".\\output\\filtered_df_googleplaystore_ex2")
    
    //ex3
    
    val df3_1 = df_googleplaystore.select(
    col("App").cast(StringType).as("App"),  // Change type and rename
    //col("Categories").cast(Array[String]).as("Categories"),
    split(col("Category"), ",").alias("Categories"),
    col("Rating").cast(DoubleType).as("Rating"),
    col("Reviews").cast(LongType).as("Reviews"),
    col("Size").cast(StringType).as("Size"),
    col("Installs").cast(StringType).as("Installs"),
    col("Type").cast(StringType).as("Type"),
    col("Price").cast(DoubleType).as("Price"),
    col("Content Rating").cast(StringType).as("Content_Rating"),
    //col("Genres").cast(Array[String]).as("Genres"),
    split(col("Genres"), ",").alias("Genres"),
    col("Last Updated").cast(StringType).as("Last_Updated"),
    col("Current Ver").cast(StringType).as("Current_Version"),
    col("Android Ver").cast(StringType).as("Minimum_Android_Version")    
    )
    
    val df3_2 = df3_1.na.fill(0, Seq("Reviews"))  // Fill NA with 0 for the specific column
    
    // Remove 'M' and convert the remaining part to double
    val dfWithDouble3 = df3_2.withColumn("Size", regexp_replace(col("Size"), "M", "").cast(DoubleType))
    
    //convert dollar to euro
    val dfWithEuro3 = dfWithDouble3.withColumn("Price", col("Price") * lit(0.9))
    
    // Convert the string to date using the to_date function
    val dfWithDate3 = dfWithEuro3.withColumn("Last_Updated", to_date(col("Last_Updated"), "MMMM d, yyyy"))
    
    // Check for duplicates in the "App" column
    val duplicates3 = dfWithDate3.groupBy("App").count().filter($"count" > 1)
    duplicates3.show()

    // If duplicates exist, remove them
    val df3 = dfWithDate3.dropDuplicates("App")

    df3.show()
    
    //ex4
    
    val df35 = df3.join(df1, Seq("App"), "right")
    
    df35.show()
    
    
    // Define the output path
    val outputPath35 = ".\\output\\googleplaystore_cleaned_ex4"
    // Repartition to a single partition
    val df35SinglePartition = df35.coalesce(1)
    
    // Save the DataFrame as a Parquet file with gzip compression
    df35SinglePartition.write
    .format("parquet")
    .option("compression", "gzip")
    .mode("overwrite")
    .save(outputPath35)
    
    
    //ex5
    
    val df4 = df35.groupBy("Genres")
    .agg(
    count("App").alias("Count"),
    avg("Rating").alias("Average_Rating"),
    avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
    )
  
    df4.show()
    
    // Define the output path
    val outputPath4 = ".\\output\\googleplaystore_metrics_ex5"
    // Repartition to a single partition
    val df4SinglePartition = df4.coalesce(1)
    
    // Save the DataFrame as a Parquet file with gzip compression
    df4SinglePartition.write
    .format("parquet")
    .option("compression", "gzip")
    .mode("overwrite")
    .save(outputPath4)
    
    
  }
}