package edu.nyu.aquery.ast

import scala.annotation.tailrec

/**
 * Basic expression in AST
 */
trait Expr extends AST[Expr] {
  def children: Seq[Expr]

  /**
   * Sequence of all subexpressions (including self) in an expression
   * @return
   */
  def allSubExpressions: Seq[Expr] = {
    @tailrec
    def loop(exprs: Seq[Expr], acc: Seq[Expr]): Seq[Expr] = exprs match {
      case Nil => acc
      case e :: ex => loop(e.children ++ ex,  e.children ++ acc)
    }
    loop(List(this), List(this))
  }
}

// Identifiers
// Used to encode things like column in a table, but also local variables in functions, amongst
// other uses
case class Id(v: String) extends Expr {
  def children = Nil
  def dotify(currAvail: Int) = (v, currAvail + 1)
  def transform(f: PartialFunction[Expr, Expr]): Expr = transform0(f)
}

/**
 * Binary operations (arithmetic and boolean)
 */
sealed abstract class BinOp

// e + e
case object Plus extends BinOp
// e - e
case object Minus extends BinOp
// e * e
case object Times extends BinOp
// e / e
case object Div extends BinOp
// e ^ e
case object Exp extends BinOp
// e < e
case object Lt extends BinOp
// e <= e
case object Le extends BinOp
// e > e
case object Gt extends BinOp
// e >= e
case object Ge extends BinOp
// e = e
case object Eq extends BinOp
// e != e
case object Neq extends BinOp
// e & e
case object Land extends BinOp
// e | e
case object Lor extends BinOp

case class BinExpr(op: BinOp, l: Expr, r: Expr) extends Expr {
  def children: Seq[Expr] = Seq(l, r)
  // uses helper object
  def dotify(parentId: Int) = BinExpr.dotify(this, parentId)
  def transform(f: PartialFunction[Expr, Expr]) = BinExpr.transform(this, f)
}


object BinExpr {
  def dotify(o: BinExpr, currAvail: Int) = {
    val opStr = o.op match {
      case Plus => "+"
      case Minus => "-"
      case Times => "*"
      case Div => "/"
      case Exp => "pow"
      case Lt => "<"
      case Le => "<="
      case Gt => ">"
      case Ge => ">="
      case Eq => "=="
      case Neq => "!="
      case Land => "&"
      case Lor => "|"
    }
    val (left, _) = o.l.dotify(currAvail)
    val (right, _) = o.r.dotify(currAvail)
    (left + opStr + right, currAvail + 1)
  }

  def transform(o: BinExpr, f: PartialFunction[Expr, Expr]): Expr = {
    // recursively transform both arguments, before transforming self
    val newL = o.l.transform(f)
    val newR = o.r.transform(f)
    BinExpr(o.op, newL, newR).transform0(f)
  }

}

/**
 * Unary operations (arithmetic and boolean)
 */
sealed abstract class UnOp
// !e
case object Not extends UnOp
// -e
case object Neg extends UnOp

case class UnExpr(op: UnOp, v: Expr) extends Expr {
  def children: Seq[Expr] = Seq(v)
  // use helper object
  def dotify(parentId: Int) = UnExpr.dotify(this, parentId)
  def transform(f: PartialFunction[Expr, Expr]): Expr = UnExpr.transform(this, f)
}

object UnExpr {
  def dotify(o: UnExpr, currAvail: Int) = {
    val opStr = o.op match {
      case Not => "!"
      case Neg => "-"
    }
    val (arg, _) = o.v.dotify(currAvail)
    (opStr + arg, currAvail + 1)
  }
  def transform(o: UnExpr, f: PartialFunction[Expr, Expr]) = {
    UnExpr(o.op, o.v.transform(f)).transform0(f)
  }
}


/**
 * Function calls
 */
trait CallExpr extends Expr {
  def f: String
  def args: Seq[Expr]
}

// f(e*)
case class FunCall(f: String, args: List[Expr]) extends CallExpr {
  def children = args
  def dotify(currAvail: Int): (String, Int) = {
    val funArgs = args.map(_.dotify(currAvail)._1).mkString(",")
    val funCall = f + "(" + funArgs + ")"
    (funCall, currAvail + 1)
  }

  def transform(fun: PartialFunction[Expr, Expr]) = {
    FunCall(f, args.map(_.transform(fun))).transform0(fun)
  }
}

/**
 * Safe indexing operators
 */
sealed trait IndexOperator
// even indices 0, 2, ...
case object Even extends IndexOperator
// odd indices 1, 3, ...
case object Odd extends IndexOperator
// every-N indices
case class Every(v: Int) extends IndexOperator
// f[EVEN/ODD/EVERY N]
case class ArrayIndex(e: Expr, ix: IndexOperator) extends CallExpr {
  def f: String = e.dotify(0)._1
  def args: Seq[Expr] = Seq(e)
  def children = args
  def dotify(currAvail: Int): (String, Int) = {
    val indexOp = ix match {
      case Even => "even"
      case Odd => "odd"
      case Every(n) => "every " + n
    }
    val label =  f + "[" + indexOp + "]"
    (label, currAvail + 1)
  }
  def transform(f: PartialFunction[Expr, Expr]): Expr = transform0(f)
}


/**
 * Control flow expression
 * CASE e
 *  WHEN e1 THEN e2
 *  [ElSE e3]
 * END
 *
 * CASE
 *  WHEN e1 THEN e2
 *  [ELSE e3]
 * END
 */
trait ControlFlowExpr extends Expr
// Pairs of condition expression and consequence expression
case class IfThen(c: Expr, t: Expr) extends Expr {
  def children = List(c, t)
  def dotify(currAvail: Int) = {
    val (ifCond, _) = c.dotify(currAvail)
    val (conseq, _) = t.dotify(currAvail)
    val nodeLabel = "if (" + ifCond + ") then " + conseq
    val node = Dot.declareNode(currAvail, nodeLabel)
    (node, currAvail + 1)
  }
  def transform(f: PartialFunction[Expr, Expr]) = IfThen(c.transform(f), t.transform(f)).transform0(f)
}
// CASE .... END
case class Case(cond: Option[Expr], when: List[IfThen], e: Option[Expr]) extends ControlFlowExpr {
  def children = cond.map(List(_)).getOrElse(Nil) ++ when ++ cond.map(List(_)).getOrElse(Nil)

  def dotify(currAvail: Int) = {
    val selfNode = Dot.declareNode(currAvail, "case")
    val (condNode, whenId) = cond match {
      case Some(ex) =>
        val (nodeLabel, nextId) = ex.dotify(currAvail + 1)
        (Dot.declareNode(currAvail + 1, nodeLabel) + Dot.declareEdge(currAvail, currAvail + 1), nextId)
      case None => ("", currAvail + 1)
    }
    val (whenNodes, elseId) = Dot.strList(when, whenId, (w: IfThen, id: Int) => {
      val edge = Dot.declareEdge(currAvail, id)
      val (node, nextAvail) = w.dotify(id)
      (node + edge, nextAvail)
    })
    val (elseNode, nextAvail) = e match {
      case Some(ex) =>
        val (nodeLabel, nextId) = ex.dotify(elseId)
        (Dot.declareNode(elseId, nodeLabel) + Dot.declareEdge(currAvail, elseId), nextId)
      case None => ("", elseId)
    }
    (selfNode + condNode + whenNodes + elseNode, nextAvail)
  }
  def transform(f: PartialFunction[Expr, Expr]) = {
    val newCond = e.map(_.transform(f))
    val newWhen = when.map(_.transform(f).asInstanceOf[IfThen])
    val newE = e.map(_.transform(f))
    Case(newCond, newWhen, newE).transform0(f)
  }
}


/**
 * Literals in expressions: int, float, string, date, boolean, timestamp
 */
trait Lit extends Expr {
  // use helper object
  def children = Nil
  def dotify(currAvail: Int) = Lit.dotify(this, currAvail)
  def transform(f: PartialFunction[Expr, Expr]) = Lit.transform(this, f)
}

case class IntLit(v: Long) extends Lit
case class FloatLit(v: Double) extends Lit
case class StringLit(v: String) extends Lit
case class DateLit(v: String) extends Lit
case class BooleanLit(v: Boolean) extends Lit
case class TimestampLit(v: String) extends Lit

object Lit {
  def dotify(l: Lit, currAvail: Int) =  {
    val label = l match {
      case IntLit(v) => v.toString
      case FloatLit(v) => v.toString
      case StringLit(v) => v
      case DateLit(v) => v
      case BooleanLit(v) => v.toString
      case TimestampLit(v) => v
    }
    (label, currAvail + 1)
  }
  def transform(l: Lit, f: PartialFunction[Expr, Expr]) = l.transform0(f)
}

/**
 * Expressions related to table. Mainly used for things like wildcard, or the virtual row id column
 */
sealed trait TableExpr extends Expr {
  def children = Nil
  def dotify(currAvail: Int) = TableExpr.dotify(this, currAvail)
  def transform(f: PartialFunction[Expr, Expr]) = TableExpr.transform(this, f)
}
// Every table can access row id with ROWID
case object RowId extends TableExpr
// t.c1
case class ColumnAccess(t: String, c: String) extends TableExpr
// *
case object WildCard extends TableExpr

object TableExpr {
  def dotify(t: TableExpr, currAvail: Int) = {
    val label = t match {
      case RowId => "ROWID"
      case ColumnAccess(a1, a2) => s"$a1.$a2"
      case WildCard => "*"
    }
    (label, currAvail + 1)
  }
  def transform(t: TableExpr, f: PartialFunction[Expr, Expr]) = t.transform0(f)
}


