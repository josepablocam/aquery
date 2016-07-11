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
  def loc = "[" + pos.line + "," + pos.column + "]"

}

/**
 * Type error
 * @param expected
 * @param found
 * @param pos
 */
case class TypeError(expected: TypeTag, found: TypeTag, pos: Position) extends AnalysisError {
  override def toString =
    "Type Error: expected " + expected + " found " + found + " at " + loc
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
    "Call Error: bad call to " + f + " at " + loc

}

/**
 * Wrong number of args to function call
 * @param f
 * @param expected
 * @param found
 * @param pos
 */
case class NumArgsError(f: String, expected: Int, found: Int, pos: Position) extends AnalysisError {
  override def toString = {
    val loc = "[" + pos.line + "," + pos.column + "]"
    "Call Error: expected " + expected + " args for " + f + " found " + found + " at " + loc
  }
}
