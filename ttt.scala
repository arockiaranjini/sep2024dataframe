import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ttt extends App {
  //def main(args: Array[String]): Unit = {
  //config
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Example")
  sparkConf.set("spark.master", "local[*]")
  // create SparkSession
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  val rdd1 = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6))
  rdd1.collect.foreach(println)
  // apply map function to RDD to double each element
  /* map applies a given function to each element in the collection and returns a
  new collection of the same type.
  For each input element map produces exactly one output element
  , meaning the resulting collection has a one - to - one relationship
  with the original collection.
    The resulting collection typically has the same structure as the original collection
    */
  val doubled = rdd1.map(x => x * 2)
  // print the result
  doubled.collect().foreach(println)
  /*flatMap also applies a function to each element in the collection but can
 return zero one or multiple elements for each input element.
   The result of flatMap is a flattened collection, meaning it doesn 't maintain the original structure
 , and the elements are combined into a single collection.
   This is particularly useful when you want to break down or expand the structure of the elements
 , such as when dealing with nested collections or sequences*/
  val sentences =spark.sparkContext.parallelize (Seq("Hello World", "Scala is great", "Spark is powerful"))
  val words = sentences.flatMap(sentence => sentence.split(" "))
  words.collect().foreach(println)
  spark.stop()
}


