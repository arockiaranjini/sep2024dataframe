

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
/////Explain runtime error
object compiletimeWithoutSafetyDF extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf =new SparkConf()
  sparkConf.set("spark.app.name","myfirstapplication")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("C:/Users/MCT/Desktop/sparkdataset/orders.csv")
  ordersDf.filter("orders_ids< 10").show //compile time not safety
  scala.io.StdIn.readLine()
  spark.stop()
}
