
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Level
import org.apache.log4j.Logger

object cachePersist extends App {

  //object wordCount extends App {
  // def main(args:Array[String])
  // {}

  Logger.getLogger("org").setLevel(Level.ERROR)
  // if any error show me otherwise no need to show


  val sc =new SparkContext("local[*]","withcache")

  val input = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/data.txt")
  input.collect.foreach(println)
  val words =input.flatMap(x => x.split(" "))
  words.collect.foreach(println)
  val wordMap = words.map(x=>(x,1))
 // val finalCount= wordMap.reduceByKey((a,b) => a+b).cache()
  //cache()=persist(StorageLevel.MEMORY_ONLY)
//  val finalCount= wordMap.reduceByKey((a,b) => a+b).persist(StorageLevel.MEMORY_ONLY)
 // val finalCount= wordMap.reduceByKey((a,b) => a+b).persist(StorageLevel.MEMORY_ONLY_SER)
 //val finalCount= wordMap.reduceByKey((a,b) => a+b).persist(StorageLevel.DISK_ONLY)
  //val finalCount= wordMap.reduceByKey((a,b) => a+b).persist(StorageLevel.MEMORY_AND_DISK)
  //val finalCount= wordMap.reduceByKey((a,b) => a+b).persist(StorageLevel.MEMORY_AND_DISK_SER)
  val finalCount= wordMap.reduceByKey((a,b) => a+b).persist(StorageLevel.MEMORY_AND_DISK_SER_2) //2 means no of replication
  finalCount.collect.foreach(println)
  val sortKeyResult =finalCount.sortByKey()//sort by key only
  sortKeyResult.collect.foreach(println)
  val sortbyResult1 = finalCount.sortBy(x=>x._1) //sort by first value
  sortbyResult1.collect.foreach(println)
  val sortbyResult2 = finalCount.sortBy(x => x._2)//sort by second value
  sortbyResult2.collect.foreach(println)
  val filterResult2=finalCount.filter(x => x._1 == "you")
  filterResult2.collect.foreach(println)
//save result into the folder
  scala.io.StdIn.readLine()
  // this means program is still running not terminated
  //it will show DAG
}
//Serialized format consumes less memory since the data is stored in a compact binary form.

  //This can be advantageous when working with large datasets that don't fit entirely in memory.

/*Deserialized format consumes more memory compared to the serialized format because it stores the data in its original, expanded form.
As a result, you might be limited by the available memory for caching large datasets.*/


/*MEMORY_ONLY: Cache the RDD or DataFrame in memory as deserialized Java objects. This is the most memory-efficient option.
MEMORY_ONLY_SER: Cache the RDD or DataFrame in memory as serialized Java objects. This is memory-efficient but may require more CPU time to deserialize.
MEMORY_AND_DISK: Cache the RDD or DataFrame in memory as deserialized Java objects, and if it doesn't fit, spill the excess to disk.
  MEMORY_AND_DISK_SER: Cache the RDD or DataFrame in memory as serialized Java objects, and if it doesn't fit, spill the excess to disk.
  DISK_ONLY: Cache the RDD or DataFrame on disk only.
MEMORY_ONLY_2, MEMORY_ONLY_SER_2, etc.: These options replicate the data on two nodes for fault tolerance.
You can replace _2 with a different number for more replication.
*/


/*Storage Level    Space used  CPU time  In memory  On-disk  Serialized   Recompute some partitions
----------------------------------------------------------------------------------------------------
MEMORY_ONLY          High        Low       Y          N        N         Y
MEMORY_ONLY_SER      Low         High      Y          N        Y         Y
MEMORY_AND_DISK      High        Medium    Some       Some     Some      N
MEMORY_AND_DISK_SER  Low         High      Some       Some     Y         N
DISK_ONLY            Low         High      N          Y        Y         N

 */


