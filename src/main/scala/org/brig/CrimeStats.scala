package org.brig

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
import org.apache.spark.sql.expressions._


object CrimeStats extends App {

  val spark = SparkSession
    .builder()
    .appName(" СrimeStats")
    .master(master = "local[*]")
    .getOrCreate()

  import spark.implicits._

  val crime_file: String = args(0)
  val offense_file: String = args(1)
  val output: String = args(2)


  // Read Crime dataset, filter out Null District
  val crime: DataFrame = spark
    .read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv(crime_file)
    .filter($"DISTRICT".isNotNull)

  crime.show(20)

  // Read Offense Codes dataset and broadcast it
  val offense: DataFrame = spark
    .read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv(offense_file)
    .withColumn("crime_type", trim(split($"NAME", "-")(0)))

  offense.show(20, false)

  // broadcast offense data
  val offense_bc = broadcast(offense)


  val crime_total = crime
    .select("INCIDENT_NUMBER", "OFFENSE_CODE", "DISTRICT", "MONTH", "Lat", "Long")
    .filter($"DISTRICT".isNotNull)
    .join(offense_bc, $"CODE" === $"OFFENSE_CODE")
    .drop("CODE")
    .cache()

  crime_total.show

  // median function

  def median(inputList: List[Long]): Long = {
    val sortedList = inputList.sorted
    val count: Int = inputList.size
    if (count % 2 == 0) {
      val l: Int = count / 2 - 1
      val r: Int = l + 1
      (sortedList(l) + sortedList(r)) / 2
    } else
      sortedList(count / 2)
  }

  // define UDF median and mkstring
  def medianUDF: UserDefinedFunction = udf((l: mutable.WrappedArray[Long]) => median(l.toList))

  def mkStringUDF: UserDefinedFunction = udf((l: mutable.WrappedArray[String]) => l.toList.mkString(", "))

  // total + month

  val tbl1 = crime_total
    .groupBy($"DISTRICT", $"MONTH")
    .agg(count($"INCIDENT_NUMBER").alias("crimes_num"))
    .groupBy($"DISTRICT")
    .agg(sum($"crimes_num").alias("crimes_total"),
      collect_list($"crimes_num").alias("month_list"))
    .withColumn("crimes_monthly", medianUDF($"month_list"))
    .drop($"month_list")

  tbl1.show()

  val window: WindowSpec = Window.partitionBy($"DISTRICT").orderBy($"crimes_num".desc)

  // crime)types and crime_nums
  val tbl2 = crime_total
    .groupBy($"DISTRICT", $"crime_type")
    .agg(count($"INCIDENT_NUMBER").alias("crimes_nums"))
    .withColumn("rank", row_number().over(window))
    .filter($"rank" < 4)
    .drop($"rank")
    .groupBy($"DISTRICT")
    .agg(collect_list($"crime_type").alias("crime_type_list"))
    .withColumn("frequent_crime_types", mkStringUDF($"crime_type_list"))
    .drop($"crime_type_list")

  tbl2.show()

  // lat and lng

  val tbl3 = crime_total
    .groupBy($"DISTRICT")
    .agg(mean($"Lat").alias("lat"), mean($"Long").alias("lng"))


  // Join and save to output
  val result: DataFrame = tbl1
    .join(tbl2, Seq("DISTRICT"))
    .join(tbl3, Seq("DISTRICT"))

  result.show(20)

  result.repartition(1).write.parquet(output)

  val ss = spark.read.parquet(output).show(20)

  spark.stop()
}



//spark-submit --master local[*] --class org.brig.CrimeStats target/scala-2.11/crime-assembly-0.0.1.jar data/crime.csv data/offense_codes.csv data/out_fl





