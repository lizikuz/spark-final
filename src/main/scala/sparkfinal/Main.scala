package sparkfinal

import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import  org.apache.spark.sql.expressions.Window

import java.util.Date

object Main {
  

  
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()
  
  
  def main(args: Array[String]): Unit = {

    
    //load warehouse data
    val warehouse   = spark.sparkContext.textFile("src/main/resources/sparkfinal/warehouse.csv")
    //create warehouse dataframe with schema
    val warehouseDataFrame = warehouseDataframeWithSchema(warehouse)
    
    //load amount data
    val amount   = spark.sparkContext.textFile("src/main/resources/sparkfinal/amount.csv")
    //create amount dataframe with schema
    val amountDataFrame = amountDataFrameWithSchema(amount)
    
    
     
    //create df with max-min-avg amounts
    val minMaxAvgPositions = amountDataFrame.groupBy("positionId").agg(max("amount").as("max"), min("amount").as("min"), avg("amount").as("avg"))
    //join with warehouses
    val joinMaxMin = minMaxAvgPositions.join(warehouseDataFrame, minMaxAvgPositions("positionId") === warehouseDataFrame("positionId"))
                  .select("warehouse", "product", "max", "min", "avg")
                  
    //show results
    joinMaxMin.show()
    
    
    
    // create df with current amounts
    val w = Window.partitionBy("positionId")
    val currentPosition = amountDataFrame.withColumn("maxEventTime", max("eventTime").over(w))
                  .filter("eventTime = maxEventTime")
                  .drop("eventTime")
                  .withColumnRenamed("maxEventTime", "eventTime")
                  .withColumnRenamed("positionId", "posId")
    //join with warehouse
    val joinCurrent = currentPosition.join(warehouseDataFrame, currentPosition("posId") === warehouseDataFrame("positionId"))
                  .select("positionId", "warehouse", "product", "amount")
  
    //show results       
    joinCurrent.show()
  
  }
  
  def warehouseDataframeWithSchema(warehouse: RDD[String]): DataFrame = {
    val whHeaderColumns = warehouse.first().split(",").to[List]
    val whSchema = warehouseSchema(whHeaderColumns)
    val whData =
      warehouse
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(line => Row(line(0).toLong,
                        line(1).toString,
                        line(2).toString,
                        new Timestamp(line(3).toLong * 1000)))
      spark.createDataFrame(whData, whSchema)
  }
  
  def amountDataFrameWithSchema(amount: RDD[String]): DataFrame = {
    val amHeaderColumns = amount.first().split(",").to[List]
    val amSchema = amountSchema(amHeaderColumns)
    val amData =
      amount
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(line => Row(line(0).toLong,
                        line(1).toDouble,
                        new Timestamp(line(2).toLong * 1000)))
    spark.createDataFrame(amData, amSchema)
  }
  
  def warehouseSchema(columnNames: List[String]): StructType = {

    var fields: List[StructField] = List(StructField(columnNames(0), LongType, false),
      StructField(columnNames(1), StringType, false),
      StructField(columnNames(2), StringType, false),
      StructField(columnNames(3), TimestampType, false))
    StructType(fields)
  }
  
  def amountSchema(columnNames: List[String]): StructType = {
    var fields: List[StructField] = List(StructField(columnNames(0), LongType, false),
      StructField(columnNames(1), DoubleType, false),
      StructField(columnNames(2), TimestampType, false))
    StructType(fields)
  }
  
}



case class AmountRow(
  positionId: Long,
  amount: BigDecimal,
  eventTime: Timestamp
)

case class WarehouseRow(
  positionId: Long,
  warehouse: String,
  product: String,
  eventTime: Timestamp
)


    /*val amountDataFrame = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", ",")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .option("inferSchema", "true")
    .option("nullValue", "null")
    .load("src/main/resources/sparkfinal/amount.csv")
    
    amountDataFrame.show()*/