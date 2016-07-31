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
    new BuiltInSummary("ABS", num_Num, false, false, false),
    new BuiltInSummary("AVG", boolOrNum_Num, false, true, true),
    new BuiltInSummary("COUNT", { case x :: Nil => TNumeric }, false, true, true),
    new BuiltInSummary("FILL", selfAndSelf_Self, false, false, false),
    new BuiltInSummary("IN", { case x :: xs if xs.forall(_.consistent(Set(x))) => TBoolean }, false, false, false),
    new BuiltInSummary("LIKE", selfAndSelf_Bool, false, false, false),
    new BuiltInSummary("BETWEEN", { case xs if xs.forall(_.consistent(num)) => TBoolean }, false, false, false),
    new BuiltInSummary("MAX", {
      case v @ x :: xs if v.forall(_.consistent(numAndBool)) =>
        if (v.forall(_.consistent(bool))) TBoolean else TNumeric
    }, false, true, true),
    new BuiltInSummary("MIN", {
      case v @ x :: xs if v.forall(_.consistent(numAndBool)) =>
        if (v.forall(_.consistent(bool))) TBoolean else TNumeric
    }, false, true, true),
    new BuiltInSummary("NOT", bool_Bool, false, false, false),
    new BuiltInSummary("NULL", { case x :: Nil  => TBoolean }, false, false, false),
    new BuiltInSummary("OR", { case v @ x :: y :: xs if v.forall(_.consistent(bool)) => TBoolean }, false, false, false),
    new BuiltInSummary("PRD", boolOrNum_Num, false, true, true),
    new BuiltInSummary("SQRT", num_Num, false, false, false),
    new BuiltInSummary("SUM",  num_Num orElse bool_Num, false, true, true),
    new BuiltInSummary("SHOW", { case x => TUnit }, false, false, false),
    new BuiltInSummary("LIST", { case x :: xs => TUnknown }, false, false, false)
  )

  def odBuiltIn(s: String, sig: Sig): BuiltInSummary = new BuiltInSummary(s, sig, true, false, true)

  val orderDependent = Set(
    odBuiltIn("AVGS", boolOrNum_Num orElse numAndBoolOrNum_Num),
    odBuiltIn("DELTAS", num_Num),
    odBuiltIn("DROP",  numAndSelf_Self),
    odBuiltIn("FILLS", self_Self orElse selfAndSelf_Self),
    // TODO: first/last should have size check
    odBuiltIn("FIRST", self_Self orElse numAndSelf_Self),
    odBuiltIn("LAST", self_Self orElse numAndSelf_Self),
    odBuiltIn("MAXS", boolOrNum_Self orElse numAndBoolOrNum_Self),
    odBuiltIn("MINS", boolOrNum_Self orElse numAndBoolOrNum_Self),
    odBuiltIn("NEXT", self_Self orElse numAndSelf_Self),
    odBuiltIn("PREV", self_Self orElse numAndSelf_Self),
    odBuiltIn("PRDS", boolOrNum_Num orElse numAndBoolOrNum_Num),
    odBuiltIn("SUMS", boolOrNum_Num orElse numAndBoolOrNum_Num),
    odBuiltIn("VARS", boolOrNum_Num orElse numAndBoolOrNum_Num)
  )

  val defaultInfo = (normal ++ orderDependent).map { x => x.f -> x }.toMap

  /**
   * Return a function info with all built-ins and default info
   * @return
   */
  def unit(): FunctionInfo = new FunctionInfo(defaultInfo)

  def apply(funs: Seq[UDF], summarize: (FunctionInfo, UDF) => FunctionInfo): FunctionInfo =
    this(unit(), funs, summarize)

  def apply(init: FunctionInfo, funs: Seq[UDF], summarize: (FunctionInfo, UDF) => FunctionInfo)
  : FunctionInfo =
    funs.foldLeft(init)((info, f) => summarize(info, f))
}
