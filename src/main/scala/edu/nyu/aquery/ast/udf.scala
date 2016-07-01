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
    val commandsNodeLabel = cs.map {
      case Left(as) => as.dotify(0)._1
      case Right(ex) => ex.dotify(0)._1
    }.mkString(";")
    val commandsNode = Dot.declareNode(currAvail + 1, commandsNodeLabel)
    (nameNode + commandsEdge + commandsNode, currAvail + 2)
  }
  // We do not optimize over functions
  def transform(f: PartialFunction[UDF, UDF]) = this
}

// assigns expression `e` to a local variable named `n`
case class Assign(n: String, e: Expr) extends AST[Assign] {
  def dotify(currAvail: Int) = {
    val lval = n + ":="
    val (rval, _) = e.dotify(currAvail)
    (lval + rval, currAvail + 1)
  }
  // we don't optimize assignments inside a function body
  def transform(f: PartialFunction[Assign, Assign]) = this
}
