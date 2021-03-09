package com.blp.wntask

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Task1 {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Task1")
    .config("spark.master", "local")
    .getOrCreate()


  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    val properties: Properties = new Properties()

    val in = new FileInputStream("application.properties")
    properties.load(in)

    val csvRawInputPath = properties.getProperty("input.file.path")

    val rdd = spark.sparkContext.textFile(csvRawInputPath)

    // Group by Genre
    val groupRdd = rdd.groupBy(_.split(";")(3))

    val data: RDD[(String, Double)] = groupRdd.map(x => {
      val row = x._2.iterator
      var global_sales = 0.0d
      var na_sales = 0.0d
      var eu_sales = 0.0d
      var jp_sales = 0.0d

      while (row.hasNext) {
        val item = row.next().split(";")
        na_sales += item(5).toDouble
        eu_sales += item(6).toDouble
        jp_sales += item(7).toDouble

        global_sales += na_sales + eu_sales + jp_sales

      }
      (x._1 -> global_sales)
    })

    val maxPair = data.sortBy(_._2, false).take(1)
    println("Highest selling Genre: %s\t\t Global Sales (in millions): %.2f".format(maxPair(0)._1, maxPair(0)._2))

    val minPair = data.sortBy(_._2, true).take(1)
    println("Lowest selling Genre: %s\t\t Global Sales (in millions): %.2f".format(minPair(0)._1, minPair(0)._2))
  }
}
