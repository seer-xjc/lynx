package org.grapheco.lynx.procedure

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.property.LynxNull

trait CallableProcedure {
  val inputs: Seq[(String, LynxType)]
  val outputs: Seq[(String, LynxType)]
  val forNull: Boolean = true

  def call(args: Seq[LynxValue]): LynxValue

  def execute(args: Seq[LynxValue]): LynxValue = {
//    if (forNull && args.size==1 && args.head == LynxNull) { LynxNull} // TODO when return null
    if (forNull && args.contains(LynxNull)) LynxNull
    else { call(args)}
  }

  def signature(name: String) = s"$name(${inputs.map(x => Seq(x._1, x._2).mkString(":")).mkString(",")})"

  def checkArgumentsNumber(actualNumber: Int): Boolean = actualNumber == inputs.size

  def checkArgumentsType(actualArgumentsType: Seq[LynxType]): Boolean = {
    (forNull && actualArgumentsType.size == 1 && actualArgumentsType.head == LynxNull.lynxType) || // forNull
      (actualArgumentsType.size == inputs.size && // not null
        inputs.map(_._2).zip(actualArgumentsType).forall{ case (except, actual) => except == actual})
  }
}
