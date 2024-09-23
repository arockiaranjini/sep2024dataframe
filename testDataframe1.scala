import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object testDataframe1 extends App{

  val sparkConf =new SparkConf()
  sparkConf.set("spark.app.name","myfirstapplication")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf = spark.read.csv("C:/Users/MCT/Desktop/sparkdataset/orders.csv")
  ordersDf.show()
 ordersDf.printSchema()
  scala.io.StdIn.readLine()
  spark.stop()
}

