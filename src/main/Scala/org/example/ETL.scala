package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime


object ETL extends driver {
 def load (): (DataFrame,DataFrame)={
     val df1 : DataFrame =  Table1.dataframe1() //extracting dataframe
     val df2 : DataFrame=  Table2.dataframe2()
     (df1,df2)

 }


  def transform(df1: DataFrame, df2: DataFrame): DataFrame ={
    val current_time = LocalDateTime.now.toString
    val percent = df1.agg(expr("percentile(score,0.99)").as("99_percentile_score"), expr("percentile(price,0.99)").as("99_percentile_price"))
    percent.show()
    val df3 = df1.withColumn("attributeCount", size(split(col("attribute"),","))-1)
      .withColumn("is_expired", when(col("expiry")>current_time,0).otherwise(1))
    val dataset_3 = df3.groupBy("genre_id").agg(avg("attributeCount") as "avg_attribute", (sum("is_expired")/count("is_expired"))*100 as "percentage_Expired",
      avg("price") as "avg_cost")
    val df4 = dataset_3.join(df2,df3("genre_id") === df2("genre_id"), "inner")
    df4

  }
  def write(finalDataframe: DataFrame): Unit = {

    finalDataframe.show()

  }
}


