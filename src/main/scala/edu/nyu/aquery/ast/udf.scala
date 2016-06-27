package edu.nyu.aquery.ast

/**
 * User-defined functions.
 * @param n Name of function
 * @param args list of formal argument names
 * @param cs list of either expressions or assignments to local variables. The result of a function
 *           is the last expression in the list.
 */
case class UDF(
  n: String,
  args: List[String],
  cs: List[Either[Assign, Expr]]) extends AST[UDF] with TopLevel {
  def dotify(currAvail: Int) = {
    val nameLabel = n + "(" + args.mkString(",") + ")"
    val nameNode = Dot.declareNode(currAvail, nameLabel)
    val commandsEdge = Dot.declareEdge(currAvail, currAvail + 1)
    val (commandsNode, nextAvail) = Dot.strList(
      cs,
      currAvail + 1,
      (e: Either[Assign, Expr], id: Int) => {
        e match {
          case Left(as) => as.dotify(id)
          case Right(ex) => (Dot.declareNode(id, ex.dotify(id)._1), id + 1)
        }
      }
    )
    (nameNode + commandsEdge + commandsNode, nextAvail)
  }
  // We do not optimize over functions
  def transform(f: PartialFunction[UDF, UDF]) = this
}

// assigns expression `e` to a local variable named `n`
case class Assign(n: String, e: Expr) extends AST[Assign] {
  def dotify(currAvail: Int) = {
    val lval = n + ":="
    val (rval, _) = e.dotify(currAvail)
    (Dot.declareNode(currAvail, lval + rval), currAvail + 1)
  }
  // we don't optimize assignments inside a function body
  def transform(f: PartialFunction[Assign, Assign]) = this
}
