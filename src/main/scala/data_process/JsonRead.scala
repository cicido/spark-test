package data_process

import java.nio.charset.MalformedInputException

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.{JSONException, JSONObject, JSONTokener}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object JsonRead {
  var hiveContext: HiveContext = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  val srcTable = "ad_recommend.odl_cpd_search_ad"
  val dstTable = "ad_recommend.odl_cpd_search_ad_unjson"

  def main(args: Array[String]): Unit = {
    initSpark("AdSearch-RevJson")
    val dt = args(0).toLong
    val df = getData(srcTable, dt)
    revJsonData(dstTable, dt, df)
  }

  def initSpark(appname: String): Unit = {
    System.setProperty("user.name", "ad_recommend")
    System.setProperty("HADOOP_USER_NAME", "ad_recommend")
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

  def revJsonDataFromFile(inFile: String, outFile: String): Unit = {
    val data = tryOpenFile(inFile)
    val imei_cols = Array("device_model", "firmware", "imei", "ip", "net", "oper_type",
      "product_type", "req_uri", "screen_size", "sn",
      "u_search_id", "u_session_id", "version_name")

    val ad_int_cols = Array("display", "kw_type")
    val ad_long_cols = Array("app_id", "kw_id", "unit_id")
    val ad_string_cols = Array("kw", "request_id")

    val algo_cols = Array("algorithm_version", "keyword")

    data.foreach(r => {
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

        val ab = new ArrayBuffer[(Array[Int], Array[Long], Array[String])]()

        try {
          val ads = event_content.getJSONArray("ads")
          for (i <- 0 until ads.length()) {
            val ad = ads.getJSONObject(i)
            val ad_int_Array = ad_int_cols.map(ad.getInt(_))
            val ad_long_Array = ad_long_cols.map(ad.getLong(_))
            val ad_string_Array = ad_string_cols.map(ad.getString(_))
            ab.append((ad_int_Array, ad_long_Array, ad_string_Array))
          }
        } catch {
          case e: JSONException => e //.printStackTrace()
        }
        println(ab.length)
      } catch {
        case e: JSONException => e.printStackTrace()
      }
    })
  }

  def getData(tbname: String, dt: BigInt): DataFrame = {
    val select_sql = s"select * from $tbname where stat_date =$dt"
    hiveContext.sql(select_sql)
  }

  val dataExmaple =
    """
      |{"device_model":"M68AA","event_content":{"ads":[{"app_id":627060,"display":1,"kw":"厂房","kw_id":2197001,"kw_type":1,"request_id":"18ec44f749afbc21a7a08dc0b6803f6b","unit_id":2847},{"app_id":3060503,"display":1,"kw":"厂房","kw_id":2197001,"kw_type":1,"request_id":"18ec44f749afbc21a7a08dc0b6803f6b","unit_id":4183},{"app_id":1664035,"display":1,"kw":"厂房","kw_id":2197001,"kw_type":1,"request_id":"18ec44f749afbc21a7a08dc0b6803f6b","unit_id":4210}],"algorithm_version":"quixey1.0","keyword":"厂房"},"firmware":"Flyme 5.2.4.0Y","imei":"1234","ip":"14.114.175.172","net":"wifi","oper_time":1479140840320,"oper_type":"APP_CPD_SEARCH","operator":"46002","product_type":"APPS","req_uri":"/apps/public/search","screen_size":"1080x1920","sn":"1111","u_search_id":"c5331adce8b4b779","u_session_id":"348d51ef-3427-4406-925e-94157eb45c43","uid":129524097,"version_code":322,"version_name":"5.4.10"}	20161115
      |{"device_model":"MX4","event_content":{"algorithm_version":"ARA1.1&BIZpart","keyword":"百度浏览器"},"firmware":"Flyme 5.1.11.0A","imei":"111111111","ip":"36.63.174.255","net":"wifi","oper_time":1479140957298,"oper_type":"APP_CPD_SEARCH","operator":"46002","product_type":"APPS","req_uri":"/apps/public/search","screen_size":"1152x1920","sn":"12","u_search_id":"6de87e331bb48426","u_session_id":"478c9847-d3fe-47a8-91d0-7ead53bdf69f","uid":127431808,"version_code":322,"version_name":"5.4.10"}	20161115
      |{"device_model":"MX4","event_content":{"ads":[{"app_id":1161911,"display":1,"kw":"洪铟八字算命","kw_id":473779,"kw_type":1,"request_id":"6938fdad79071f15dd895131c5cae379","unit_id":2478},{"app_id":1782697,"display":1,"kw":"洪铟八字算命","kw_id":473779,"kw_type":1,"request_id":"6938fdad79071f15dd895131c5cae379","unit_id":2719},{"app_id":3098649,"display":0,"kw":"洪铟八字算命","kw_id":473779,"kw_type":1,"request_id":"6938fdad79071f15dd895131c5cae379","unit_id":3897},{"app_id":822055,"display":0,"kw":"洪铟八字算命","kw_id":473779,"kw_type":1,"request_id":"6938fdad79071f15dd895131c5cae379","unit_id":3339},{"app_id":1912705,"display":0,"kw":"洪铟八字算命","kw_id":473779,"kw_type":1,"request_id":"6938fdad79071f15dd895131c5cae379","unit_id":2694}],"algorithm_version":"ARA1.1&BIZpart","keyword":"洪铟八字算命"},"firmware":"Flyme OS 5.1.4.0A","imei":"866654022354499","ip":"211.143.146.235","net":"wifi","oper_time":1479426889123,"oper_type":"APP_CPD_SEARCH","product_type":"APPS","req_uri":"/apps/public/search","screen_size":"1080x1920","sn":"972587f7","u_search_id":"","u_session_id":"6e9fdc6950657e7986dca4daf8cc15e36da9b10d","version_code":244,"version_name":"5.1.12"}
    """.stripMargin

  def revJsonData(tbname: String, dt: BigInt, df: DataFrame): Unit = {
    val imei_string_cols = Array("device_model", "firmware", "imei", "ip", "net", "oper_type",
      "operator", "product_type", "req_uri", "screen_size", "sn",
      "u_search_id", "u_session_id", "version_name")
    val imei_long_cols = Array("oper_time", "uid", "version_code")
    val imei_cols = imei_string_cols ++ imei_long_cols

    val ad_int_cols = Array("display", "kw_type")
    val ad_long_cols = Array("app_id", "kw_id", "unit_id")
    val ad_string_cols = Array("kw", "request_id")
    val ad_cols = ad_int_cols ++ ad_long_cols ++ ad_string_cols

    val algo_cols = Array("algorithm_version", "keyword")

    val all_cols = imei_cols ++ ad_cols ++ algo_cols
    val nulldata = all_cols.map(r => "")

    val fields = all_cols.map(fieldName =>
      StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val revRdd = df.flatMap(r => {
      var data = Array((nulldata))
      val jsonText = r.getAs[String]("vcontent")
      val jsonTokener = new JSONTokener(jsonText)
      try {
        val logJSONObject = jsonTokener.nextValue().asInstanceOf[JSONObject]
        val imeiArray = imei_string_cols.map(r => {
          try {
            logJSONObject.getString(r)
          } catch {
            case e:JSONException => ""
          }
        }) ++
          imei_long_cols.map(r => {
            try {
              logJSONObject.getLong(r).toString
            } catch {
              case e:JSONException => ""
            }
          })

        val event_content = logJSONObject.getJSONObject("event_content")
        val algoArray = algo_cols.map(r => {
          try{
            event_content.getString(r)
          }catch {
            case e:JSONException => ""
          }
        })

        val ab = new ArrayBuffer[Array[String]]()

        try {
          val ads = event_content.getJSONArray("ads")
          for (i <- 0 until ads.length()) {
            val ad = ads.getJSONObject(i)
            val ad_int_Array = ad_int_cols.map(r =>{
              try {
                ad.getInt(r).toString
              }catch{
                case e:JSONException => ""
              }
            })

            val ad_long_Array = ad_long_cols.map(r =>{
              try {
                ad.getLong(r).toString
              }catch{
                case e:JSONException => ""
              }
            })

            val ad_string_Array = ad_string_cols.map(r =>{
              try{
                ad.getString(r)
              }catch {
                case e:JSONException => ""
              }
            })

            ab.append(ad_int_Array ++ ad_long_Array ++ ad_string_Array)
          }
        } catch {
          case e: JSONException => e //.printStackTrace()
        }
        if (ab.length == 0) {
          data = Array(imeiArray ++ ad_cols.map(r => "") ++ algoArray)
        } else {
          data = ab.map(r => {
            (imeiArray ++ r ++ algoArray)
          }).toArray
        }
      } catch {
        case e: JSONException => println(jsonText)
      }
      data
    }).map(r => Row.fromSeq(r))

    val tmptable = "dxp_tmp_table"
    hiveContext.createDataFrame(revRdd, schema).registerTempTable(tmptable)

    val cols_string = all_cols.map(r => r + " string").mkString(",")
    val create_table_sql: String = s"create table if not exists $tbname " +
      s"($cols_string) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $tbname partition(stat_date = $dt) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
  }
}


