/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._

/* Implicits */
import org.apache.spark.sql.test.TestSQLContext._

case class TestData(key: Int, value: String)

object TestData {
  val testData = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => TestData(i, i.toString))).toSchemaRDD
  testData.registerTempTable("testData")

  val negativeData = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => TestData(-i, (-i).toString))).toSchemaRDD
  negativeData.registerTempTable("negativeData")

  case class LargeAndSmallInts(a: Int, b: Int)
  val largeAndSmallInts =
    TestSQLContext.sparkContext.parallelize(
      LargeAndSmallInts(2147483644, 1) ::
      LargeAndSmallInts(1, 2) ::
      LargeAndSmallInts(2147483645, 1) ::
      LargeAndSmallInts(2, 2) ::
      LargeAndSmallInts(2147483646, 1) ::
      LargeAndSmallInts(3, 2) :: Nil).toSchemaRDD
  largeAndSmallInts.registerTempTable("largeAndSmallInts")

  case class TestData2(a: Int, b: Int)
  val testData2 =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
      TestData2(1, 2) ::
      TestData2(2, 1) ::
      TestData2(2, 2) ::
      TestData2(3, 1) ::
      TestData2(3, 2) :: Nil, 2).toSchemaRDD
  testData2.registerTempTable("testData2")

  val negativeData2 = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => TestData2(i, (-i)))).toSchemaRDD
  negativeData2.registerTempTable("negativeData2")


  case class DecimalData(a: BigDecimal, b: BigDecimal)

  val decimalData =
    TestSQLContext.sparkContext.parallelize(
      DecimalData(1, 1) ::
      DecimalData(1, 2) ::
      DecimalData(2, 1) ::
      DecimalData(2, 2) ::
      DecimalData(3, 1) ::
      DecimalData(3, 2) :: Nil).toSchemaRDD
  decimalData.registerTempTable("decimalData")

  case class BinaryData(a: Array[Byte], b: Int)
  val binaryData =
    TestSQLContext.sparkContext.parallelize(
      BinaryData("12".getBytes(), 1) ::
      BinaryData("22".getBytes(), 5) ::
      BinaryData("122".getBytes(), 3) ::
      BinaryData("121".getBytes(), 2) ::
      BinaryData("123".getBytes(), 4) :: Nil).toSchemaRDD
  binaryData.registerTempTable("binaryData")

  case class TestData3(a: Int, b: Option[Int])
  val testData3 =
    TestSQLContext.sparkContext.parallelize(
      TestData3(1, None) ::
      TestData3(2, Some(2)) :: Nil).toSchemaRDD
  testData3.registerTempTable("testData3")

  val emptyTableData = logical.LocalRelation('a.int, 'b.int)

  case class UpperCaseData(N: Int, L: String)
  val upperCaseData =
    TestSQLContext.sparkContext.parallelize(
      UpperCaseData(1, "A") ::
      UpperCaseData(2, "B") ::
      UpperCaseData(3, "C") ::
      UpperCaseData(4, "D") ::
      UpperCaseData(5, "E") ::
      UpperCaseData(6, "F") :: Nil).toSchemaRDD
  upperCaseData.registerTempTable("upperCaseData")

  case class LowerCaseData(n: Int, l: String)
  val lowerCaseData =
    TestSQLContext.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
      LowerCaseData(2, "b") ::
      LowerCaseData(3, "c") ::
      LowerCaseData(4, "d") :: Nil).toSchemaRDD
  lowerCaseData.registerTempTable("lowerCaseData")

  case class ArrayData(data: Seq[Int], nestedData: Seq[Seq[Int]])
  val arrayData =
    TestSQLContext.sparkContext.parallelize(
      ArrayData(Seq(1,2,3), Seq(Seq(1,2,3))) ::
      ArrayData(Seq(2,3,4), Seq(Seq(2,3,4))) :: Nil)
  arrayData.registerTempTable("arrayData")

  case class MapData(data: scala.collection.Map[Int, String])
  val mapData =
    TestSQLContext.sparkContext.parallelize(
      MapData(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      MapData(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      MapData(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      MapData(Map(1 -> "a4", 2 -> "b4")) ::
      MapData(Map(1 -> "a5")) :: Nil)
  mapData.registerTempTable("mapData")

  case class StringData(s: String)
  val repeatedData =
    TestSQLContext.sparkContext.parallelize(List.fill(2)(StringData("test")))
  repeatedData.registerTempTable("repeatedData")

  case class twoData(s: String, i:Int)
  val nullableRepeatedData =
    TestSQLContext.sparkContext.parallelize(
      List.fill(2)(twoData(null,2)) ++
      List.fill(2)(twoData("test",2))++
        List.fill(4)(twoData(null,3)) ++
        List.fill(4)(twoData("test",3)),2)
  nullableRepeatedData.registerTempTable("nullableRepeatedData")

  case class NullInts(a: Integer)
  val nullInts =
    TestSQLContext.sparkContext.parallelize(
      NullInts(1) ::
      NullInts(2) ::
      NullInts(3) ::
      NullInts(null) :: Nil
    )
  nullInts.registerTempTable("nullInts")

  val allNulls =
    TestSQLContext.sparkContext.parallelize(
      NullInts(null) ::
      NullInts(null) ::
      NullInts(null) ::
      NullInts(null) :: Nil)
  allNulls.registerTempTable("allNulls")

  case class NullStrings(n: Int, s: String)
  val nullStrings =
    TestSQLContext.sparkContext.parallelize(
      NullStrings(1, "abc") ::
      NullStrings(2, "ABC") ::
      NullStrings(3, null) :: Nil)
  nullStrings.registerTempTable("nullStrings")

  case class TableName(tableName: String)
  TestSQLContext.sparkContext.parallelize(TableName("test") :: Nil).registerTempTable("tableName")

  val unparsedStrings =
    TestSQLContext.sparkContext.parallelize(
      "1, A1, true, null" ::
      "2, B2, false, null" ::
      "3, C3, true, null" ::
      "4, D4, true, 2147483644" :: Nil)

  case class TimestampField(time: Timestamp)
  val timestamps = TestSQLContext.sparkContext.parallelize((1 to 3).map { i =>
    TimestampField(new Timestamp(i))
  })
  timestamps.registerTempTable("timestamps")

  case class IntField(i: Int)
  // An RDD with 4 elements and 8 partitions
  val withEmptyParts = TestSQLContext.sparkContext.parallelize((1 to 4).map(IntField), 8)
  withEmptyParts.registerTempTable("withEmptyParts")

  case class Person(id: Int, name: String, age: Int)
  case class Salary(personId: Int, salary: Double)
  val person = TestSQLContext.sparkContext.parallelize(
    Person(0, "mike", 30) ::
    Person(1, "jim", 20) :: Nil)
  person.registerTempTable("person")
  val salary = TestSQLContext.sparkContext.parallelize(
    Salary(0, 2000.0) ::
    Salary(1, 1000.0) :: Nil)
  salary.registerTempTable("salary")

  case class ComplexData(m: Map[Int, String], s: TestData, a: Seq[Int], b: Boolean)
  val complexData =
    TestSQLContext.sparkContext.parallelize(
      ComplexData(Map(1 -> "1"), TestData(1, "1"), Seq(1), true)
        :: ComplexData(Map(2 -> "2"), TestData(2, "2"), Seq(2), false)
        :: Nil).toSchemaRDD
  complexData.registerTempTable("complexData")

  val rdd = TestSQLContext.sparkContext.parallelize(Row(1,Seq("a",2,3),3.4) :: Row(3,Seq(1,2,3),1,2) :: Nil)
  //val schema = StructType(
  //      StructField ("a",IntegerType, true) ::
  //      StructField ("b",ArrayType(IntegerType), true) ::
  //      StructField ("c",DoubleType, true) :: Nil
  //)
  val rowRDD = TestSQLContext.sparkContext.parallelize(
    Row(Map("a" -> 1,"b" -> Map("bb" -> 3),"c" -> 3.4)) :: Row(Map("a" -> 3,"b" -> Seq(1,2),"c" -> 1, "d" -> 2)) :: Nil)

  val mix = TestSQLContext.applySchema(rowRDD,AnyTypeObj)

  mix.registerTempTable("myrows")

  val rowRDD1 = TestSQLContext.sparkContext.parallelize(
    Row(1,1) :: Row(1,3.5) :: Nil)

  val mix1 = TestSQLContext.applySchema(rowRDD1,StructType(Seq(StructField("a",IntegerType),StructField("b",DoubleType))))

  mix1.registerTempTable("rows")

  val rowRDD2 = TestSQLContext.sparkContext.parallelize(
    //Row(2,3) :: Row(3,4) :: Nil)
    Row(Map("a" -> 2,"b" -> 3,"c" -> true)) :: Row(Map("a" -> 3.0,"b" -> 7.0, "c"-> false)) :: Nil)

  val mix2 = TestSQLContext.applySchema(rowRDD2,AnyTypeObj)//StructType(StructField("a",IntegerType)::StructField("b",IntegerType)::Nil))

  mix2.registerTempTable("IntsAsFloats")

  val testD =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(1, 2) ::
        TestData2(2, 5) ::
        TestData(2, "3.2") ::
        TestData(3, "1") ::
        TestData(3, "2") :: Nil, 2)
  testD.registerTempTable("testData77")

  case class TestData8(a : Int, b: Float)
  val testD8 =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(2, 5) ::
        TestData2(1, 2) ::
        TestData2(2, 5) ::
        TestData8(2, 3.0.toFloat) ::
        TestData8(3, 3.41.toFloat) ::
        TestData8(3, 3.2.toFloat) :: Nil, 2)
  testD8.registerTempTable("testDataIntFloat")

  case class TestDataString(a: Int, b: String)

  val testH =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(2, 5) ::
        TestData2(1, 2) ::
        TestData2(2, 5) ::
        TestData8(2, 3.0.toFloat) ::
        TestData8(3, 3.41.toFloat) ::
        TestData8(3, 3.2.toFloat) ::
        TestDataString(1,"a" ) ::
        TestDataString(4,"x") ::
        TestDataString(1,"a") ::
        TestDataString(2, "b") ::
        Nil, 2)
  testH.registerTempTable("testHeterogeneousData")

  val rowSchemaless = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => Row(Map ("key" -> i,"value" -> i.toString ))))
  val testDataSchemaless = TestSQLContext.applySchema(rowSchemaless,AnyTypeObj)
  testDataSchemaless.registerTempTable("testDataSchemaless")





  val openRDD = TestSQLContext.sparkContext.parallelize((
    Row(1, 2.3, List(1), List(3,4), Map("a" -> 3.0,"b" -> 7.0, "c"-> "option")) :: Nil) ++
    (1 to 100).map(i => Row(1,i.toDouble,i.toString,List(i),null)) ++
    Seq(Row(2, 3.0, "a"    , List(2,3), Map("x" -> 2,"y" -> 3,"z" -> true))) ++
    (1 to 100).map(i => Row(2,i.toDouble,"xx",List(2,i),Map("a"->(i+1), "y" -> (i*2).toDouble))),
    2)

  val sType = OpenStructType(
    StructField("number",IntegerType) ::
    StructField("float",DoubleType) ::
    StructField("any", AnyTypeObj) ::
    StructField("array", ArrayType(IntegerType))::Nil)

  val openSchemaRDD = TestSQLContext.applySchema(openRDD,sType)

  openSchemaRDD.registerTempTable("openTable")


}
