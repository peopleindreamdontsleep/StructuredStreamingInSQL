package com.sqlsteam

import shapeless._
import syntax.std.traversable._
import syntax.std.tuple._

object Test {

  def main(args: Array[String]): Unit = {

    case class data(a: Int, b: Int, c: Int, d: Int, e: Int)
    type DATA = Int :: Int :: Int :: Int :: Int :: HNil
    val arr = "1\t2\t3\t4\t5".split('\t').map(_.toInt)
    val mydata = data.tupled(arr.toHList[DATA].get.tupled)
    print(mydata)
  }

}
