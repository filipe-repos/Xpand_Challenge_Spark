package session

import org.apache.spark.sql.SparkSession

object ScalaSparkSession {
  
  def sparkSession(): SparkSession = {
    
    SparkSession.builder().appName("ScalaSparkChallenge").master("local").getOrCreate()
  }
  
}