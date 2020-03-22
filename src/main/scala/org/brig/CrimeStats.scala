package org.brig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types._



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


  /* def median(inputList: List[Long]): Long = {
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

*/
  // total + month


  val crimes_by_monthly = crime
      .groupBy("DISTRICT", "YEAR","MONTH")
      .agg(count($"INCIDENT_NUMBER").alias("CRIMES_IN_MONTH"))
      .groupBy("DISTRICT")
      .agg(callUDF("percentile_approx", $"CRIMES_IN_MONTH", lit(0.5)).as("CRIMES_MONTHLY"))
      .withColumnRenamed("DISTRICT", "DISTRICT_MONTHLY")

  crimes_by_monthly.show()


  val window_district = Window.partitionBy("DISTRICT")


  // crime)types and crime_nums
  val crimes_by_types = crime.join(broadcast(offense), crime("OFFENSE_CODE") === offense("CODE"))
    .groupBy("DISTRICT", "NAME")
    .agg(count($"NAME").alias("NAME_DESC"))
    .withColumn("rank", row_number().over(window_district.orderBy(desc("NAME_DESC"))))
    .where("rank <= 3")
    .groupBy("DISTRICT")
    .agg(collect_list("NAME").alias("FREQ_CRIME_TYPES"))
    .withColumnRenamed("DISTRICT", "DISTRICT_BY_TYPES")

  crimes_by_types.show()


  // Join and save to output
  val result = crime_total
    .join(crimes_by_monthly, crime_total("DISTRICT") === crimes_by_monthly("district_monthly"),"left_outer")
    .join(crimes_by_types, crime_total("DISTRICT") === crimes_by_types("district_name_by_list"),"left_outer")
    .select("DISTRICT", "CRIMES_MONTHLY", "FREQ_CRIME_TYPES", "Lat", "Long")
    .orderBy("DISTRICT")
    .repartition(1)
    .write.format("parquet").mode(SaveMode.Overwrite).save(output)


  val ss = spark.read.parquet(output).show(20)

  spark.stop()
}



//spark-submit --master local[*] --class org.brig.CrimeStats target/scala-2.11/crime-assembly-0.0.1.jar data/crime.csv data/offense_codes.csv data/out_fl

