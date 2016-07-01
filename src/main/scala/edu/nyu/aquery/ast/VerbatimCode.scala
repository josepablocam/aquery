package edu.nyu.aquery.ast

/**
 * Used to hold literal code to be inserted into the output
 * @param c
 */
case class VerbatimCode(c: String) extends AST[VerbatimCode] with TopLevel {
  val CHAR_LIMIT = 100
  def dotify(currAvail: Int) = {
    // limit for how much verbatim code to show in dot graph
    val label = c.take(CHAR_LIMIT)
    (Dot.declareNode(currAvail, label), currAvail + 1)
  }
  // we will never optimize verbatim code
  def transform(f: PartialFunction[VerbatimCode, VerbatimCode]) = this
}