package com.ruozedata

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkSQLDemo {

  // $example on:create_ds$
  case class Person(name: String, age: Long)
  // $example on:create_ds$

  def main(args: Array[String]): Unit = {
    //开启SparkSession
    //    $example on: init_session$
    val spark = SparkSession
      .builder()
      .appName("SparkSQLDemo")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
//    $example off: init_session$


//    runBasicDataFrameDemo(spark)
//    runDatasetCreationDemo(spark)
//    runInferSchemaDemo(spark)
    runProgrammaticSchemaDemo(spark)

    //关闭SparkSeesion
    spark.stop()

  }

  private def runBasicDataFrameDemo(spark: SparkSession) = {

    val df = spark.read.json("/Users/hadoop/app/spark/examples/src/main/resources/people.json")

    //Displays the content of the DataFrame to stdout
    df.show()

    //Print the schema in a tree format
    df.printSchema()

    //Select only the "name" column
    df.select("name").show()

    //This import is needed to use the $-notation
    import spark.implicits._
    df.select($"name", $"age" + 1).show()

    //Select people older than 21
    df.select($"age" > 21).show()

    //Count people by age
    df.groupBy("age").count().show()


    //$example on: global_temp_view$
    //Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()

    //Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    //Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("select * from global_temp.people").show

    //Global temporary view is cross-session
    spark.newSession().sql("select * from global_temp.people").show()

  }


  private def runDatasetCreationDemo(spark: SparkSession) = {

//    A container for a [[Dataset]], used for implicit conversions in Scala.
//    To use this, import implicit conversions in SQL:
    import spark.implicits._

    // .toDS() -> 这是用括号声明的，以防止Scala编译器将`rdd.toDS（“1”）`视为调用此toDS然后应用于返回的数据集。

    //Encoder are created for case classes (为case class 创建编码器)
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    //Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).foreach(println(_))//.collect()

    //DataFrames can be converted to a Dataset by providing a class. Mapping will bedone by name
    val path = "/Users/hadoop/app/spark/examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()


  }



  private def runInferSchemaDemo(spark: SparkSession) = {

//    $example on: schema_inferring$
    //For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    //Create an RDD of Person objects from a text file, convert it to a DataFrame
    val peopleDF = spark.sparkContext
      .textFile("/Users/hadoop/app/spark/examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(x => Person(x(0), x(1).trim.toInt))
      .toDF()

    //Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    //SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("select name, age from people where age between 13 and 19")

    //The columns of a row in the result can be accessed by field index
    //(结果中的行的列可以通过字段索引访问)
    teenagersDF.map(teenager => s"Name: ${teenager(0)}").show()


    //or by field name
    teenagersDF.map(teenager => s"Name: ${teenager.getAs[String]("name")}").show()



    //No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    //(Dataset[Map[K,V]] 没有预定义的编码器, 显式定义)
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    //Primitive types and case classes can be also defined as
    //(原始类型和case类也可以定义为隐式val )
    //implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    //row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager =>
      teenager.getValuesMap[Any](List("name", "age"))
    ).foreach(println(_))//.collect()


//    $example off: schema_inferring$

  }


  private def runProgrammaticSchemaDemo(spark: SparkSession) = {

    import spark.implicits._
//    $example on: programmatic_schema$

    //Create an RDD
    val peopleRDD = spark.sparkContext.textFile("/Users/hadoop/app/spark/examples/src/main/resources/people.txt")

    //The schema is encoded in a string
    val schemaString = "name age"

    //Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    //Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    //Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    //SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("select name from people")

    //The results of SQL queries are DataFrames and support all the normal RDD operations
    //The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => s"Name: ${attributes(0)}").show()


//    $exmaple off: programmatic_schema$
  }












}
