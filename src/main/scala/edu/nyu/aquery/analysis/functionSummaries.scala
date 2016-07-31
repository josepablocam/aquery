package edu.nyu.aquery.analysis

import AnalysisTypes._

/**
 * Function summaries contain summary information for a given function. We stuck with
 * normal classes here to easily extend with new properties in the future.
 */
sealed abstract class FunctionSummary {
  def f: String
  def signature: PartialFunction[Seq[TypeTag], TypeTag]
  def orderDependent: Boolean
  def remOrderDependence: Boolean
  def usesAggregate: Boolean
}

/**
 * Summary for a built-in function
 * @param f function name
 * @param signature partial function representing type signature
 * @param orderDependent
 */
class BuiltInSummary(
  val f: String,
  val signature: PartialFunction[Seq[TypeTag], TypeTag],
  val orderDependent: Boolean = false,
  val remOrderDependence: Boolean = false,
  val usesAggregate: Boolean = false) extends FunctionSummary

/**
 * Summary for an UDF
 * @param f function name
 * @param signature partial function representing type signature
 * @param orderDependent
 */
class UDFSummary(
  val f: String,
  val signature: PartialFunction[Seq[TypeTag], TypeTag],
  val orderDependent: Boolean = false,
  val remOrderDependence: Boolean = false,
  val usesAggregate: Boolean = false) extends FunctionSummary