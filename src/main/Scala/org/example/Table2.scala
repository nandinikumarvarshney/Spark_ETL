package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SaveMode
object Table2 {

  val sp = SparkSession.builder().appName("Spark demo").master("local[*]").getOrCreate()

  import sp.implicits._
  def dataframe2(): DataFrame = {


    val random = scala.util.Random

    //SECOND DATAFRAME CREATION
    val totalrows = 100
    val database = new ListBuffer[(Int, String)]
    for (i <- 1 to totalrows) {
      val genre_id = i
      val a = random.nextInt(10) + 1
      val genre_name = random.alphanumeric.take(a).mkString
      val row = (genre_id, genre_name)
      database += row
    }

    val df_genre: DataFrame = database.toDF("genre_id", "genre_name")
    df_genre.write
      .format("jdbc").mode(SaveMode.Overwrite)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/newdb")
      .option("dbtable", "genre_table")
      .option("user", "root")
      .option("password", "root")
      .save()

    writeDataframe(df_genre, "genre_table")
    readDataFrame("genre_table")

  }

  def writeDataframe(dataFrame: DataFrame, Name: String): Unit = {
    dataFrame.write
      .format("jdbc").mode(SaveMode.Overwrite)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/newdb")
      .option("dbtable", Name)
      .option("user", "root")
      .option("password", "root")
      .save()
  }

  def readDataFrame(Name: String): DataFrame = {
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
