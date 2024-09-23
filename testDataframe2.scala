import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object testDataFrame2 extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  val ordersDf = spark.read //1st action read the csv
    .option("header", true)
    .option("inferSchema", true) //2nd action infer the schema//not used in production//manually specify the schema
    .csv("C:/Users/MCT/Desktop/sparkdataset/orders.csv")
  ordersDf.show() //3rd action
  ordersDf.printSchema()
  scala.io.StdIn.readLine()
  spark.stop()
}

