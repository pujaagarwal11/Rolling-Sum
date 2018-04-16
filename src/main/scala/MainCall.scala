import org.apache.spark.sql.{SQLContext, UserDefinedFunction}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object MainCall {

  def main(args: Array[String]): Unit = {
    println("hello world")
    val tdf = SetContext.sqlCtx.read.format("csv").option("header", "false").option("delimiter", "\t").load(getClass.getResource("/data.txt").getPath)


    val rdd = tdf.rdd
    val framedDF = SetContext.sqlCtx.createDataFrame(rdd, SetSchemaJob.appSchema)

    //val windowSpecDs=framedDF.groupBy(window(framedDF.col("Time"),"60 seconds"))

    val windowSpec = Window.partitionBy().orderBy("Time")
    val df = framedDF
      .withColumn("diff_With_Prev_Row", col("Time") - ((lag("Time", 1).over(windowSpec))))
      .withColumn("diff_Sec_With_Prev_Row", when(col("diff_With_Prev_Row").isNull, 0).otherwise(col("diff_With_Prev_Row")))
      .withColumn("diff_Sec_With_Prev_Row_Int", col("diff_Sec_With_Prev_Row").cast("Int"))
      .withColumn("valueInt", col("Value").cast("Double"))
      .withColumn("No_of_Observations", Extract.countObservation(col("diff_Sec_With_Prev_Row_Int"), when(lag(col("diff_Sec_With_Prev_Row_Int").cast("Int"), 1).over(windowSpec).isNull, 0)
        .otherwise(lag(col("diff_Sec_With_Prev_Row_Int").cast("Int"), 1).over(windowSpec))))
      .withColumn("Roll_Sum", Extract.rollSumValue(col("diff_Sec_With_Prev_Row_Int"), col("valueInt"), when(lag(col("Value").cast("Double"), 1).over(windowSpec).isNull, 0).otherwise(lag(col("Value").cast("Double"), 1).over(windowSpec))))
      .withColumn("Min_value", Extract.minValue(col("valueInt"), when(lag(col("Value").cast("Double"), 1).over(windowSpec).isNull, 0).otherwise(lag(col("Value").cast("Double"), 1).over(windowSpec))))
      .withColumn("Max_value", Extract.minValue(col("valueInt"), when(lag(col("Value").cast("Double"), 1).over(windowSpec).isNull, 0)
        .otherwise(lag(col("Value").cast("Double"), 1).over(windowSpec))))
      .drop("diff_With_Prev_Row")
      .drop("diff_Sec_With_Prev_Row")
      .drop("valueInt")
    df.show(40)

  }

  object SetSchemaJob {

    val appSchema = StructType(List(
      StructField("Time", StringType, true),
      StructField("Value", StringType, true)))
  }


  object SetContext {
    val sparkConf = new SparkConf().setAppName("app1").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlCtx: SQLContext = new HiveContext(sc)
  }

  object Extract {

    val minValue = udf((currentValue: Double, observationValue: Double) => getMinValue(currentValue, observationValue))

    def getMinValue(currentValue: Double, observationValue: Double): Double = {
      currentValue match {
        case x if x <= observationValue => currentValue
        case _ => {
          observationValue match {
            case x if x == 0.0 => currentValue
            case _ => observationValue.toDouble
          }
        }
      }
    }

    val maxValue = udf((currentValue: Double, observationValue: Double) => getMaxValue(currentValue, observationValue))

    def getMaxValue(currentValue: Double, observationValue: Double): Double = {

      currentValue match {
        case x if x <= observationValue => currentValue
        case _ => {
          observationValue match {
            case x if x == 0.0 => currentValue
            case _ => observationValue.toDouble
          }
        }
      }
    }

    val rollSumValue: UserDefinedFunction = udf((diff_Sec_With_Prev_Row_Int: Int, valueInt: Double, previousValue: Double) => getRollSum(diff_Sec_With_Prev_Row_Int, valueInt, previousValue))

    def getRollSum(diff_Sec_With_Prev_Row_Int: Int, valueInt: Double, previousValue: Double) = {
      diff_Sec_With_Prev_Row_Int match {
        case x if x >= 60 => valueInt
        case _ => val roll: Double = valueInt

          val rollPreviousValue: Double = previousValue
          val finalRoll = {
            rollPreviousValue match {
              case x if x == 0.0 => roll
              case _ => roll + rollPreviousValue
            }
          }
          finalRoll
      }
    }

    val accum = SetContext.sc.accumulator(0)
    val countObservation: UserDefinedFunction = udf((diff_Sec_With_Prev_Row_Int: Int, previousValue: Int) => getObservationNumber(diff_Sec_With_Prev_Row_Int, previousValue))

    def getObservationNumber(diff_Sec_With_Prev_Row_Int: Int, previousValue: Int) = {


      if (diff_Sec_With_Prev_Row_Int < 60 && previousValue < 60 && (diff_Sec_With_Prev_Row_Int + previousValue) < 60) {
        accum += (1)
        accum.value
        // /diff_Sec_With_Prev_Row_Int+previousValue
      }
      else {
        accum.value = 1
        accum.value
      }
    }
  }

}
