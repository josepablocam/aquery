package edu.nyu.aquery.analysis

import AnalysisTypes._

/**
 * Function summaries contain summary information for a given function. We stuck with
 * normal classes here to easily extend with new properties in the future.
 */
abstract class FunctionSummary {
  def f: String
  def numArgs = argTypes.size
  def argTypes: Seq[Set[TypeTag]]
  def returnType: TypeTag
  def orderDependent: Boolean
}

/**
 * Summary for a built-in function
 * @param f
 * @param argTypes
 * @param returnType
 * @param orderDependent
 */
class BuiltInSummary(
  val f: String,
  val argTypes: Seq[Set[TypeTag]],
  val returnType: TypeTag = TUnknown,
  val orderDependent: Boolean = false) extends FunctionSummary

/**
 * Summary for an UDF
 * @param f
 * @param argTypes
 * @param returnType
 * @param orderDependent
 */
class UDFSummary(
  val f: String,
  val argTypes: Seq[Set[TypeTag]],
  val returnType: TypeTag = TUnknown,
  val orderDependent: Boolean = false) extends FunctionSummary