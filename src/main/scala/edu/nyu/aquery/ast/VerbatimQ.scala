package edu.nyu.aquery.ast

/**
 * Used to hold literal q code to be inserted into the output
 * @param q
 */
case class VerbatimQ(q: String) extends AST[VerbatimQ] with TopLevel {
  val CHAR_LIMIT = 100
  def dotify(currAvail: Int) = {
    // limit for how much verbatim q to show in dot grapj
    val label = q.take(CHAR_LIMIT)
    (Dot.declareNode(currAvail, label), currAvail + 1)
  }
  // we will never optimize verbatim q
  def transform(f: PartialFunction[VerbatimQ, VerbatimQ]) = this
}