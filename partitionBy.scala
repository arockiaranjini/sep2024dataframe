import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object partitionBy extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("header", true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/orders.csv")
    .load

  print("orderdf has=" +ordersDf.rdd.getNumPartitions+"\n")
  //inside the folder 4 files
 // val ordersRep = ordersDf.repartition(4)
  // print("ordersRep has=" +ordersRep.rdd.getNumPartitions+"\n")
  //ordersRep.write
    //
  //
  ordersDf.write
    .format("csv")
    //partitionby //may be use two columns
    .partitionBy("order_status")
    .option("maxRecordsPerFile", 2000)
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/partition03SEP")
    .save()

  //ordersDf.show()
  // ordersDf.printSchema()
  //scala.io.StdIn.readLine()
  spark.stop()
}

