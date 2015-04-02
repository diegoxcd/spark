package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.ScalaReflection

/**
 * Created by diegoxcd on 4/1/15.
 */
object SQLPlusPlusTypes {

  // The conversion for integral and floating point types have a linear widening hierarchy:
  private val typePrecedence =
    Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType.Unlimited, StringType, ArrayType, StructType)

  def typeOrder(t1: DataType): Int = typePrecedence.indexOf(t1)
  def coerceAnyNumeric(evalE:Any, t1:DataType): (Any,DataType) ={
    t1 match {
      case t: AnyType => evalE match {
        case exp: String =>
          try {
            (exp.toDouble, DoubleType)
          }catch {
            case e: NumberFormatException => (exp,StringType)
          }
        case e => (e,ScalaReflection.typeOfObject(e))
      }
      case t: NumericType => (evalE,t)
      case other => sys.error(s"Type $other does not support numeric operations")
    }
  }
  def coerceAny(evalE:Any, t1:DataType): (Any,DataType) = {
    t1 match {
      case t: AnyType => (evalE,ScalaReflection.typeOfObject(evalE))
      case t => (evalE, t)
    }
  }
  def convertToType(value: Any, from: NumericType, to: NumericType) : Any = {
    if (from == to) {
      value
    } else
      to match {
        case ByteType    => from.numeric.asInstanceOf[Numeric[Any]].toInt(value).toByte
        case ShortType   => from.numeric.asInstanceOf[Numeric[Any]].toInt(value).toShort
        case IntegerType => from.numeric.asInstanceOf[Numeric[Any]].toInt(value)
        case LongType    => from.numeric.asInstanceOf[Numeric[Any]].toLong(value)
        case FloatType   => from.numeric.asInstanceOf[Numeric[Any]].toFloat(value)
        case DoubleType  => from.numeric.asInstanceOf[Numeric[Any]].toDouble(value)
        case t:DecimalType => try {
          changePrecision(Decimal(from.numeric.asInstanceOf[Numeric[Any]].toDouble(value)), t)
        } catch {
          case _: NumberFormatException => null
        }
      }

  }
  private[this] def changePrecision(value: Decimal, decimalType: DecimalType): Decimal = {
    decimalType match {
      case DecimalType.Unlimited =>
        value
      case DecimalType.Fixed(precision, scale) =>
        if (value.changePrecision(precision, scale)) value else null
    }
  }
}