package json_test

import scala.collection.mutable.ArrayBuffer

/**
  * Created by duanxiping on 2016/10/18.
  */
object JsonTest {

  def main(args: Array[String]) {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    val arr = ArrayBuffer(("a", 1), ("b", 2), ("c", 3))
    //val arr = new ArrayBuffer[(String,Int)]()
    val arrobj = ("bidlist" -> arr.toList.map { w =>
      (("word" -> w._1) ~ ("tag" -> w._2))
    })
    println(compact(render(arrobj)))

    /*
    val aa = compact((render(arrobj) \ "bidlist" \ "word") (0))
    println(aa)

    //another extract type
    val bb: List[(String, Int)] = for {
      JObject(bidlist) <- render(arrobj)
      JField("word", JString(word)) <- bidlist
      JField("tag", JInt(age)) <- bidlist
    } yield (word, age.toInt)

    bb.foreach(r => {
      println(r._1 + "," + r._2)
    })
    */
  }
}

