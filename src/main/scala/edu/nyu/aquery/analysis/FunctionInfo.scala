package edu.nyu.aquery.analysis
import edu.nyu.aquery.ast.UDF

import AnalysisTypes._

/**
 * Maintains function information that is commonly used throughout the analysis package. It is
 * effectively a wrapper around a map from function name to function summary
 *
 * The companion object contains definitions for built-ins
 */
class FunctionInfo(val info: Map[String, FunctionSummary]) {
  def apply(f: String, numArgs: Int): Option[FunctionSummary] = info.get(FunctionInfo.asName(f, numArgs))
  def apply(f: String): Option[FunctionSummary] = info.get(f)
  def write(f: String, summary: FunctionSummary): FunctionInfo = new FunctionInfo(info.updated(f, summary))
}

object FunctionInfo {
  val normal = Set(
    new BuiltInSummary("ABS", List(num), TNumeric, false),
    new BuiltInSummary("AVG", List(num), TNumeric, false),
    new BuiltInSummary("COUNT",  List(unk), TNumeric, false),
    new BuiltInSummary("FILL",  List(unk), TUnknown, false),
    new BuiltInSummary("IN",  List(unk, unk), TBoolean, false),
    new BuiltInSummary("Like",  List(unk, unk), TBoolean, false),
    new BuiltInSummary("BETWEEN",  List(num, num, num), TBoolean, false),
    new BuiltInSummary("MAX",  List(numAndBool), TNumeric, false),
    new BuiltInSummary("MIN",  List(numAndBool), TNumeric, false),
    new BuiltInSummary("NOT",  List(bool), TBoolean, false),
    new BuiltInSummary("NULL",  List(unk), TBoolean, false),
    new BuiltInSummary("OR", List(bool, bool), TBoolean, false),
    new BuiltInSummary("PRD",  List(numAndBool), TNumeric, false),
    new BuiltInSummary("SQRT",  List(num), TNumeric, false),
    new BuiltInSummary("SUM",  List(numAndBool), TNumeric, false)
  )

  val orderDependent = Set(
    // avgs(vec)
    new BuiltInSummary("AVGS",  List(numAndBool), TNumeric, true),
    // avgs(2, vec) overload
    new BuiltInSummary("AVGS",  List(numAndBool, numAndBool), TNumeric, true),
    new BuiltInSummary("DELTAS",  List(num), TNumeric, true),
    new BuiltInSummary("DROP",  List(num, unk), TNumeric, true),
    new BuiltInSummary("FILLS",  List(unk), TUnknown, true),
    // first(vec)
    new BuiltInSummary("FIRST",  List(unk), TUnknown, true),
    // first(10, vec)
    new BuiltInSummary("FIRST",  List(num, unk), TUnknown, true),
    // last(vec)
    new BuiltInSummary("LAST",  List(unk), TUnknown, true),
    // last(10, vec)
    new BuiltInSummary("LAST",  List(num, unk), TUnknown, true),
    // maxs(vec)
    new BuiltInSummary("MAXS",  List(numAndBool), TNumeric, true),
    // maxs(2, vec) overload
    new BuiltInSummary("MAXS",  List(num, numAndBool), TNumeric, true),
    // mins(vec)
    new BuiltInSummary("MINS",  List(numAndBool), TNumeric, true),
    // mins(2, vec) overload
    new BuiltInSummary("MINS",  List(num, numAndBool), TNumeric, true),
    // prev(vec)
    new BuiltInSummary("PREV",  List(unk), TUnknown, true),
    // prev(2, vec) overload
    new BuiltInSummary("PREV",  List(num, unk), TUnknown, true),
    new BuiltInSummary("PRD",  List(numAndBool), TNumeric, true),
    // prev(2, vec) overload
    new BuiltInSummary("PRDS",  List(num, numAndBool), TUnknown, true),
    // sums(vec)
    new BuiltInSummary("SUMS",  List(numAndBool), TUnknown, true),
    // sums(2, vec) overload
    new BuiltInSummary("SUMS",  List(num, numAndBool), TUnknown, true),
    new BuiltInSummary("VARS",  List(num, num), TUnknown, true)
  )

  private val SEP_CHAR = '!'

  /**
   * Combine function name and arguments to create a name
   * @param f
   * @param n
   * @return
   */
  def asName(f: String, n: Int): String = f + SEP_CHAR + n

  // include version w/ and w/o num of arguments to find issues at call
  val defaultInfo = (normal ++ orderDependent).flatMap { x =>
    List(asName(x.f.toUpperCase, x.numArgs) -> x, x.f -> x)
  }.toMap

  /**
   * Return a function info with all built-ins and default info
   * @return
   */
  def unit(): FunctionInfo = new FunctionInfo(defaultInfo)

  def apply(funs: Seq[UDF], summarize: (FunctionInfo, UDF) => FunctionInfo) =
    funs.foldLeft(unit())((info, f) => summarize(info, f))
}
