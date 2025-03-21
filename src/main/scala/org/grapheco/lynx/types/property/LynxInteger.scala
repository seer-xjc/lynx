package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.{IntegerType, LTInteger, LynxValue}

/**
 * @ClassName LynxInteger
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxInteger(v: Long) extends LynxNumber {
  def value: Long = v

  def number: Number = v

  def +(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxInteger(v + v2)
      case LynxFloat(v2) => LynxFloat(v + v2)
    }
  }

  def -(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxInteger(v - v2)
      case LynxFloat(v2) => LynxFloat(v - v2)
    }
  }

  // TODO the type of result
  override def *(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxInteger(v*v2) // TODO: over
      case LynxFloat(v2) => LynxFloat(v*v2)
    }
  }

  override def /(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxFloat(v/v2)
      case LynxFloat(v2) => LynxFloat(v/v2)
    }
  }

  def %(that: LynxNumber): LynxInteger = {
    that match {
      case LynxInteger(v2)=> LynxInteger(v%v2)
    }
  }

  def lynxType: IntegerType = LTInteger
}
