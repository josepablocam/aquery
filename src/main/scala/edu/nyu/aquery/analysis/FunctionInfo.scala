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
  def apply(f: String): Option[FunctionSummary] = info.get(f)
  def write(f: String, summary: FunctionSummary): FunctionInfo =
    new FunctionInfo(info.updated(f, summary))
}

object FunctionInfo {
  type Sig = PartialFunction[Seq[TypeTag], TypeTag]
  // numeric to numeric
  val num_Num: Sig = { case v :: Nil if v.consistent(num) => TNumeric }
  // boolean to boolean
  val bool_Bool: Sig = { case v :: Nil if v.consistent(bool) => TBoolean }
  // self to self
  val self_Self: Sig = { case v :: Nil => v }
  // boolean to numeric
  val bool_Num: Sig = { case v :: Nil if v.consistent(bool) => TNumeric }

  // multiple types for single argument
  val boolOrNum_Self: Sig = { case v :: Nil if v.consistent(numAndBool) => v }
  val boolOrNum_Num: Sig = { case v :: Nil if v.consistent(numAndBool) => TNumeric }

  // Multiple arguments
  val numAndSelf_Self: Sig = { case a :: v :: Nil if a.consistent(num) => v }
  val selfAndSelf_Self: Sig  = { case a :: v :: Nil if a.consistent(Set(v)) => v }
  val selfAndSelf_Bool: Sig = { case a :: b :: Nil if a.consistent(Set(b)) => TBoolean }
  val numAndBoolOrNum_Num: Sig =
    { case a :: v :: Nil if a.consistent(num) && v.consistent(numAndBool) => TNumeric}
  val numAndBoolOrNum_Self: Sig =
    { case a :: v :: Nil if a.consistent(num) && v.consistent(numAndBool) => v }


  val normal = Set(
    new BuiltInSummary("ABS", num_Num, false),
    new BuiltInSummary("AVG", boolOrNum_Num, false),
    new BuiltInSummary("COUNT", { case x :: Nil => TNumeric }, false),
    new BuiltInSummary("FILL", selfAndSelf_Self, false),
    new BuiltInSummary("IN", { case x :: xs if xs.forall(_.consistent(Set(x))) => TBoolean }, false),
    new BuiltInSummary("LIKE", selfAndSelf_Bool, false),
    new BuiltInSummary("BETWEEN", { case xs if xs.forall(_.consistent(num)) => TBoolean }, false),
    new BuiltInSummary("MAX", boolOrNum_Self, false),
    new BuiltInSummary("MIN", boolOrNum_Self, false),
    new BuiltInSummary("NOT", bool_Bool, false),
    new BuiltInSummary("NULL", { case x :: Nil  => TBoolean }, false),
    new BuiltInSummary("OR", { case xs if xs.forall(_.consistent(bool)) => TBoolean } , false),
    new BuiltInSummary("PRD", boolOrNum_Num, false),
    new BuiltInSummary("SQRT", num_Num, false),
    new BuiltInSummary("SUM",  num_Num orElse bool_Num, false)
  )

  val orderDependent = Set(
    new BuiltInSummary("AVGS", boolOrNum_Num orElse numAndBoolOrNum_Num, true),
    new BuiltInSummary("DELTAS", num_Num, true),
    new BuiltInSummary("DROP",  numAndSelf_Self, true),
    new BuiltInSummary("FILLS", self_Self orElse selfAndSelf_Self, true),
    // TODO: first/last should have size check
    new BuiltInSummary("FIRST", self_Self orElse numAndSelf_Self, true),
    new BuiltInSummary("LAST", self_Self orElse numAndSelf_Self, true),
    new BuiltInSummary("MAXS", boolOrNum_Self orElse numAndBoolOrNum_Self, true),
    new BuiltInSummary("MINS", boolOrNum_Self orElse numAndBoolOrNum_Self, true),
    new BuiltInSummary("NEXT", self_Self orElse numAndSelf_Self, true),
    new BuiltInSummary("PREV", self_Self orElse numAndSelf_Self, true),
    new BuiltInSummary("PRDS", boolOrNum_Num orElse numAndBoolOrNum_Num, true),
    new BuiltInSummary("SUMS", boolOrNum_Num orElse numAndBoolOrNum_Num, true),
    new BuiltInSummary("VARS", boolOrNum_Num orElse numAndBoolOrNum_Num, true)
  )

  val defaultInfo = (normal ++ orderDependent).map { x => x.f -> x }.toMap

  /**
   * Return a function info with all built-ins and default info
   * @return
   */
  def unit(): FunctionInfo = new FunctionInfo(defaultInfo)

  def apply(funs: Seq[UDF], summarize: (FunctionInfo, UDF) => FunctionInfo) =
    funs.foldLeft(unit())((info, f) => summarize(info, f))
}
