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
}

/**
 * Type error
 * @param expected
 * @param found
 * @param pos
 */
case class TypeError(expected: TypeTag, found: TypeTag, pos: Position) extends AnalysisError {
  override def toString = {
    val loc = "[" + pos.line + "," + pos.column + "]"
    "Type Error: expected " + expected + " found " + found + " at " + loc
  }
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
