import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger

object testDataframe4 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf =new SparkConf()
  sparkConf.set("spark.app.name","myfirstapplication")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  val ordersDf = spark.read  //1st action read the csv
    .option("header",true)
    .option("inferSchema",true)//2nd action infer the schema//not used in production//manually specify the schema
    .csv("C:/Users/MCT/Desktop/sparkdataset/orders.csv")
  //driver convert higher level code to rdd and send to executors
  val groupedOrdersDf = ordersDf
    .repartition(4)      //transformation
    .where("order_customer_id>10000") //wide transformation
    .select("order_id","order_customer_id") //transformation
    .groupBy("order_customer_id") //wide transformation
    .count() //transformation
  groupedOrdersDf.foreach(x=>{
    println(x)  //Executors
  })

  scala.io.StdIn.readLine()
  spark.stop()
}

