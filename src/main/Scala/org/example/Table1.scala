package org.example
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, types}

import java.time.LocalDateTime
import scala.collection.JavaConversions._
import java.util.UUID
import scala.collection.mutable.ListBuffer
object Table1 {
  val sp = SparkSession.builder().appName("Spark demo").master("local[*]").getOrCreate()

  import sp.implicits._
  def dataframe1(): DataFrame = {

    val schema = new ListBuffer[(String, Int, Float, String, Long, String, Int)]
    val random = scala.util.Random
    val numofRows = 10000

    def generateRandomListofStrings(a: Int): String = {
      var string_created = " "
      val length = random.nextInt(10) + 1
      for (j <- 1 to length) {
        //string_created += random.nextString(a)
        string_created += random.alphanumeric.take(a).mkString + ","
      }
      return string_created
    }

    for (i <- 1 to numofRows) {
      val uuid = UUID.randomUUID().toString
      val score = random.nextInt(1000) + 1
      val price = random.nextFloat() * 100
      val a = random.nextInt(10) + 1
      val attribute = generateRandomListofStrings(a)
      val creationTimeStamp = System.currentTimeMillis
      val expiry = if (i >= 5000) LocalDateTime.now.toString else LocalDateTime.now.plusYears(2).toString
      val genre_id = random.nextInt(100) + 1

      val row = (uuid, score, price, attribute, creationTimeStamp, expiry, genre_id)
      schema += row
    }

    val df: DataFrame = schema.toDF("uuid", "score", "price", "attribute", "creationTimeStamp", "expiry", "genre_id")
    writeDataframe(df,"spark_table")
    readDataFrame("spark_table")
    df

  }
  def writeDataframe(dataFrame: DataFrame, Name: String): Unit ={
        dataFrame.write
          .format("jdbc").mode(SaveMode.Overwrite)
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("url", "jdbc:mysql://localhost:3306/newdb")
          .option("dbtable", Name)
          .option("user", "root")
          .option("password", "root")
          .save()
  }
  def readDataFrame(Name: String): DataFrame ={
    val dataFrame: DataFrame = sp.read
              .format("jdbc")
              .option("driver", "com.mysql.cj.jdbc.Driver")
              .option("url", "jdbc:mysql://localhost:3306/newdb")
              .option("dbtable", Name)
              .option("user", "root")
              .option("password", "root")
              .load()

    dataFrame
  }
}
