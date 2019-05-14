package com.ruozedata

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object UserDefinedTypedAggregation {

 case class Employee(name: String, salary: Long)
 case class Average(var sum: Long, var count: Long)


 object MyAverage extends Aggregator[Employee, Average, Double] {

  //A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)

  //Commine two values to produce a new value. For performance, the function may modify `buffer`
  //and return it instead of constructiong a new object
  def reduce(buffer: Average, employee: Employee): Average = {
   buffer.sum += employee.salary
   buffer.count += 1
   buffer
  }

  //Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
   b1.sum += b2.sum
   b1.count += b2.count
   b1
  }

  //Transform the ouput of the reduction
  def finish(reducetion: Average): Double = reducetion.sum.toDouble / reducetion.count

  //Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product

  //Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
 }

// $example off: type_custom_aggregation$


 def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL user-defined Datasets aggregation example")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val ds = spark.read.json("/Users/hadoop/app/spark/examples/src/main/resources/employees.json").as[Employee]
  ds.show()

  val averageSalary = MyAverage.toColumn.name("average_salary")
  val result = ds.select(averageSalary)
  result.show()



  spark.stop()
 }

}
