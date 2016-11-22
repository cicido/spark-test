package data_process

import java.nio.charset.MalformedInputException

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.{JSONException, JSONObject, JSONTokener}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object jsonRead{
  var hiveContext: HiveContext = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  val srcTable = "ad_recommend.odl_cpd_search_ad"
  val dstTable = "ad_recommend.odl_cpd_search_ad_unjson"
  def main(args: Array[String]): Unit = {
    initSpark("AdSearch-RevJson")
    val dt = args(0).toLong
    val df = getData(srcTable, dt)
    revJsonData(dstTable,dt,df)
  }

  def initSpark(appname: String): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appname)
    sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    sqlContext = new SQLContext(sc)
  }

  //try to open file with different decode format
  def tryOpenFile(filename: String): Array[String] = {
    try {
      Source.fromFile(filename, "utf8").getLines().toArray
    } catch {
      case e: MalformedInputException => Source.fromFile(filename, "cp936").getLines().toArray
    }
  }

  def revJsonDataFromFile(inFile:String, outFile:String): Unit ={
    val data = tryOpenFile(inFile)
    val imei_cols = Array("device_model","firmware","imei","ip","net","oper_type",
      "product_type","req_uri", "screen_size","sn",
      "u_search_id","u_session_id","version_name")

    val ad_int_cols = Array("display","kw_type")
    val ad_long_cols = Array("app_id","kw_id","unit_id")
    val ad_string_cols = Array("kw","request_id")

    val algo_cols = Array("algorithm_version","keyword")

    data.foreach(r=>{
      val jsonText = r.split("\t")(0)
      val jsonTokener = new JSONTokener(jsonText)
      try {
        val logJSONObject = jsonTokener.nextValue().asInstanceOf[JSONObject]
        val imeiArray = imei_cols.map(logJSONObject.getString(_))
        println("imei Array:")
        imeiArray.foreach(println)

        val event_content = logJSONObject.getJSONObject("event_content")

        val algoArray = algo_cols.map(event_content.getString(_))
        println("algo Array:")
        algoArray.foreach(println)

        val ab = new ArrayBuffer[(Array[Int],Array[Long],Array[String])]()

        try {
          val ads = event_content.getJSONArray("ads")
          for(i <- 0 until ads.length()){
            val ad = ads.getJSONObject(i)
            val ad_int_Array = ad_int_cols.map(ad.getInt(_))
            val ad_long_Array = ad_long_cols.map(ad.getLong(_))
            val ad_string_Array = ad_string_cols.map(ad.getString(_))
            ab.append((ad_int_Array,ad_long_Array,ad_string_Array))
          }
        }catch{
          case e:JSONException => e//.printStackTrace()
        }
        println(ab.length)
      } catch {
        case e: JSONException => e.printStackTrace()
      }
    })
  }

  def getData(tbname:String, dt:BigInt): DataFrame ={
    val select_sql = "select * from $tbname where stat_date =$dt limit 1000"
    hiveContext.sql(select_sql)
  }

  def revJsonData(tbname:String, dt:BigInt, df: DataFrame): Unit ={
    val imei_cols = Array("device_model","firmware","imei","ip","net","oper_type",
      "product_type","req_uri", "screen_size","sn",
      "u_search_id","u_session_id","version_name")

    val ad_int_cols = Array("display","kw_type")
    val ad_long_cols = Array("app_id","kw_id","unit_id")
    val ad_string_cols = Array("kw","request_id")

    val algo_cols = Array("algorithm_version","keyword")

    val all_cols = imei_cols ++ ad_int_cols ++ ad_long_cols ++ ad_string_cols ++ algo_cols
    val nulldata = all_cols.map(r=>"")

    val fields = all_cols.map(fieldName =>
      StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val revRdd = df.flatMap(r=> {
      var data = Array((nulldata))
      val jsonText = r.getAs[String]("vcontent")
      val jsonTokener = new JSONTokener(jsonText)
      try {
        val logJSONObject = jsonTokener.nextValue().asInstanceOf[JSONObject]
        val imeiArray = imei_cols.map(logJSONObject.getString(_))

        val event_content = logJSONObject.getJSONObject("event_content")
        val algoArray = algo_cols.map(event_content.getString(_))

        val ab = new ArrayBuffer[Array[String]]()

        try {
          val ads = event_content.getJSONArray("ads")
          for (i <- 0 until ads.length()) {
            val ad = ads.getJSONObject(i)
            val ad_int_Array = ad_int_cols.map(ad.getInt(_).toString)
            val ad_long_Array = ad_long_cols.map(ad.getLong(_).toString)
            val ad_string_Array = ad_string_cols.map(ad.getString(_))
            ab.append(ad_int_Array ++ ad_long_Array ++ ad_string_Array)
          }
        } catch {
          case e: JSONException => e //.printStackTrace()
        }
        if (ab.length == 0) {
          data = Array(imeiArray ++ ad_int_cols.map(r => "") ++ ad_long_cols.map(r => "") ++
            ad_string_cols.map(r => "") ++ algoArray)
        } else {
          data = ab.map(r => {
            (imeiArray ++ r ++ algoArray)
          }).toArray
        }
      } catch {
        case e: JSONException => e.printStackTrace()
      }
      data
    }).map(Row(_))
    val tmptable = "dxp_tmp_table"
    hiveContext.createDataFrame(revRdd, schema).registerTempTable(tmptable)

    val cols_string = all_cols.map(r=>r+" string").mkString(",")
    val create_table_sql: String = s"create table if not exists $tbname " +
      "($cols_string) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $tbname partition(stat_date = $dt) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
  }
}


