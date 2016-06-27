package edu.nyu.aquery.ast

import scala.util.parsing.input.Positional


/**
 * This abstract class describes all nodes in our AST
 * and is explicitly broken out so developers can extend
 * this class when implementing new AST nodes.
 *
 * Note that it extends positional, as we want to provide line/column
 * related error information to user. If you extend this and add nodes
 * that are not used in parsing, but rather in analysis, it is considered
 * good form to copy the position information associated with the node that
 * creates the new construct. This allows downstream error reporting to relate
 * back to the source file.
 */
abstract class AST[A <: AST[_]] extends Positional {
  self: A =>
  /**
   * Return Graphviz representation, useful for debugging.
   * @param currAvail: currently available integer identifier (should be used to declare the
   *                 current graph node)
   * @return string representation in dot-format (new line separated by convention), and the
   *         next available integer identifier for nodes to come
   */
  def dotify(currAvail: Int): (String, Int)

  /**
   * Helper wrapper around f.applyOrElse(_, identity[_]), meant to be the base case call
   * for transformations
   * @param f
   * @return
   */
  protected[aquery] def transform0(f: PartialFunction[A, A]): A = f.applyOrElse(this, identity[A])

  /**
   * Nodes should implement a recursive transform definition, which calls
   * transform on each member and then transform0 on the overall argument
   * @param f
   * @return
   */
  def transform(f: PartialFunction[A, A]): A
}