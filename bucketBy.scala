//bucketBy works when we say saveAsTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Level
import org.apache.log4j.Logger
//session 11


object bucketBy extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    //to integrate hive with spark
    .config("spark.sql.sources.bucketing.enabled", true)
    .enableHiveSupport()
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/orders.csv")
    .load
  //create own databse
  //C:\apple%20intelli\movie\spark-warehouse\newretail.db
  spark.sql("create database if not exists newretails")

  ordersDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(4,"order_customer_id")
    .sortBy("order_customer_id")
    .saveAsTable("newretails.orders")

 //spark.catalog.listTables("newretail")
 // ordersDf.createOrReplaceTempView("orders")
  //val df2 = spark.sql("SELECT * FROM newretail.orders")
  //println(df2.rdd.getNumPartitions)
  //df2.show()
  spark.stop()

}
