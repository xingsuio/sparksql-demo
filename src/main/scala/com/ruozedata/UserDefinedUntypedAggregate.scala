package com.ruozedata

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object UserDefinedUntypedAggregate {

//  $example on: untyped_custom_aggregations$
  object MyAverage extends UserDefinedAggregateFunction {

    //Data types of input arguments of this aggregate function
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    //Data types of values in the aggregation buffer
    def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }

    //The data type of the returned value
    def dataType: DataType = DoubleType

    //Whether this function always return s the same output on the identical input
    def deterministic: Boolean = true


    //  """
    //    |Initializes the given aggregation buffer.
    //    |The buffer itself is a `Row` that in addition to
    //    |standard method like retrieving a value at an index (e.g., get(), getBoolean()),
    //    |providesthe opportunity to update its values.
    //    |Note that arrays andmaps inside the buffer are still ummutable.
    //  """
    def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L } //Updates the given aggregation buffer `buffer` with new input data from `input`
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //isNullAt() -> Checks whether the value at position i is null.
     if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
     }
    }

    //Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(1) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // Calcuates the final result
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }
//  $example off: untyped_custom_aggregation$

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL user-defined DataFrames aggregation example")
      .getOrCreate()


//    $eeample on: untyped_custom_aggregation$
    //Register the function to access it
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("/Users/hadoop/app/spark/examples/src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()



    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()


    spark.stop()
  }
}

