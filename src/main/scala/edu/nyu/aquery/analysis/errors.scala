package edu.nyu.aquery.analysis

import edu.nyu.aquery.analysis.AnalysisTypes.TypeTag

import scala.util.parsing.input.Position

/**
 * Errors that can arise during analysis stages.
 */
sealed abstract class AnalysisError {
  /**
   * Position in source code
   * @return
   */
  def pos: Position
  def loc(p: Position = pos) = "[" + p.line + "," + p.column + "]"

}

/**
 * Type error
 * @param expected
 * @param found
 * @param pos
 */
case class TypeError(expected: TypeTag, found: TypeTag, pos: Position) extends AnalysisError {
  override def toString =
    "Type Error: expected " + expected + " found " + found + " at " + loc()
}

/**
 * An error at a function call site (whether built-in or UDF). Unfortunately the error message
 * here is pretty limited, since we check calls using partial functions, it is not clear what
 * the error message would be in the general case (and making it specific is probably not a good
 * use of time).
 * @param f function name
 * @param pos location of bad call
 */
case class BadCall(f: String, pos: Position) extends AnalysisError {
  override def toString =
    "Call Error: bad call to " + f + " at " + loc()

}

/**
 * Wrong number of args to function call
 * @param f
 * @param expected
 * @param found
 * @param pos
 */
case class NumArgsError(f: String, expected: Int, found: Int, pos: Position) extends AnalysisError {
  override def toString =
    "Call Error: expected " + expected + " args for " + f + " found " + found + " at " + loc()
}

/**
 * A table (name, correlation name, or both) is duplicated
 * @param t1 first table name
 * @param t2 second table name
 * @param pos position of either error
 */
case class DuplicateTableName(t1: String, t2: String, pos: Position, pos2: Position)
  extends AnalysisError {
  override def toString =
    "Table Error: Duplicate table " + t1 + " at " + loc(pos) + " and " + t2 + " at " + loc(pos2)
}

/**
 * Unknown table/correlation name used in a query body in a column access (i.e. t.c)
 * @param ca column access
 * @param pos position of reference
 */
case class UnknownCorrName(ca: String, pos: Position) extends AnalysisError {
  override def toString =
    "Table Error: Unknown correlation name in " + ca + " at " + loc()
}

/**
 * Ambiguous column access used in an expression
 * e.g select t.c1 from t t1, t t2
 * @param ca
 * @param pos
 */
case class AmbigColAccess(ca: String, pos: Position) extends AnalysisError {
  override def toString =
    "Table Error: Ambiguous column access in " + ca + " at " + loc()
}

/**
 * Illegal expression used as sort column. For now, we only allow normal column (Id(_)) or
 * a column access such as t.c1. Similarly, using correlation-based access, ROWID or Wildcard
 * in a construct that is not a query.
 * @param s
 * @param pos
 */
case class IllegalExpr(s: String, pos: Position) extends AnalysisError {
  override def toString =
    "Expr Error: Illegal expression in " + s + " at " + loc()
}
