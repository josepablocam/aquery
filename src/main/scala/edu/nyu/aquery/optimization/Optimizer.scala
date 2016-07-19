package edu.nyu.aquery.optimization

import edu.nyu.aquery.ast.TopLevel

/**
 * Base optimizer trait
 */
trait Optimizer {
  /**
   * Program to optimize (used for analysis as well)
   * @return
   */
  def input: Seq[TopLevel]

  /**
   * Optimizations requested (if empty, should default to all optimizations available in module)
   * @return
   */
  def optims: Seq[String]

  /**
   * Top-level optimization routine. Returns optimized version of `input`
   * @return
   */
  def optimize: Seq[TopLevel]

  /**
   * String description of optimizer along with names of specific optimizations and their
   * explanations
   * @return
   */
  def description: String
}
