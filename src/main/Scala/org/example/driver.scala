package org.example
import org.apache.spark.sql.{DataFrame, SparkSession}


trait driver {

  def load (): (DataFrame,DataFrame)
  def transform(df1: DataFrame, df2: DataFrame): DataFrame
  def write(finalDataframe: DataFrame)


}
