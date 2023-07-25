package org.example

object Main {
  def main(args: Array[String]): Unit = {
     val (df1,df2)  = ETL.load()
     val finalDataset = ETL.transform(df1,df2)
     ETL.write(finalDataset)
     
  }
}
