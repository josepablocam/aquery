package edu.nyu.aquery.ast

/**
 * Basic expression in AST
 */
trait Expr extends AST[Expr]

// Identifiers
// Used to encode things like column in a table, but also local variables in functions, amongst
// other uses
case class Id(v: String) extends Expr {
  def dotify(currAvail: Int) = (v, currAvail + 1)
  def transform(f: PartialFunction[Expr, Expr]): Expr = transform0(f)
}

/**
 * Binary operations (arithmetic and boolean)
 */
trait BinaryExpr extends Expr {
  // for convenience, we want common names for the two arguments
  def l: Expr
  def r: Expr
  // uses helper object
  def dotify(parentId: Int) = BinaryExpr.dotify(this, parentId)
  def transform(f: PartialFunction[Expr, Expr]) = BinaryExpr.transform(this, f)
}

// e + e
case class Plus(l: Expr, r: Expr) extends BinaryExpr
// e - e
case class Minus(l: Expr, r: Expr) extends BinaryExpr
// e * e
case class Times(l: Expr, r: Expr) extends BinaryExpr
// e / e
case class Div(l: Expr, r: Expr) extends BinaryExpr
// e ^ e
case class Exp(l: Expr, r: Expr) extends BinaryExpr
// e < e
case class Lt(l: Expr, r: Expr) extends BinaryExpr
// e <= e
case class Le(l: Expr, r: Expr) extends BinaryExpr
// e > e
case class Gt(l: Expr, r: Expr) extends BinaryExpr
// e >= e
case class Ge(l: Expr, r: Expr) extends BinaryExpr
// e = e
case class Eq(l: Expr, r: Expr) extends BinaryExpr
// e != e
case class Neq(l: Expr, r: Expr) extends BinaryExpr
// e & e
case class Land(l: Expr, r: Expr) extends BinaryExpr
// e | e
case class Lor(l: Expr, r: Expr) extends BinaryExpr


object BinaryExpr {
  def dotify(o: BinaryExpr, currAvail: Int) = {
    val opStr = o match {
      case Plus(_, _) => "+"
      case Minus(_, _) => "-"
      case Times(_, _) => "*"
      case Div(_, _) => "/"
      case Exp(_, _) => "pow"
      case Lt(_, _) => "<"
      case Le(_, _) => "<="
      case Gt(_, _) => ">"
      case Ge(_, _) => ">="
      case Eq(_, _) => "=="
      case Neq(_, _) => "!="
      case Land(_, _) => "&"
      case Lor(_, _) => "|"
    }
    val (left, _) = o.l.dotify(currAvail)
    val (right, _) = o.r.dotify(currAvail)
    (left + opStr + right, currAvail + 1)
  }

  def transform(o: BinaryExpr, f: PartialFunction[Expr, Expr]): Expr = {
    // recursively transform both arguments, before transforming self
    val newL = o.l.transform(f)
    val newR = o.r.transform(f)
    o match {
      case Plus(_, _) => Plus(newL, newR).transform0(f)
      case Minus(_, _) => Minus(newL, newR).transform0(f)
      case Times(_, _) => Times(newL, newR).transform0(f)
      case Div(_, _) => Div(newL, newR).transform0(f)
      case Exp(_, _) => Exp(newL, newR).transform0(f)
      case Lt(_, _) => Lt(newL, newR).transform0(f)
      case Le(_, _) => Le(newL, newR).transform0(f)
      case Gt(_, _) => Gt(newL, newR).transform0(f)
      case Ge(_, _) => Ge(newL, newR).transform0(f)
      case Eq(_, _) => Eq(newL, newR).transform0(f)
      case Neq(_, _) => Neq(newL, newR).transform0(f)
      case Land(_, _) => Land(newL, newR).transform0(f)
      case Lor(_, _) => Lor(newL, newR).transform0(f)
    }
  }

}

/**
 * Unary operations (arithmetic and boolean)
 */
trait UnaryExpr extends Expr {
  // convenience of same name for argument
  def v: Expr
  // use helper object
  def dotify(parentId: Int) = UnaryExpr.dotify(this, parentId)
  def transform(f: PartialFunction[Expr, Expr]): Expr = UnaryExpr.transform(this, f)
}
// !e
case class Not(v: Expr) extends UnaryExpr
// -e
case class Neg(v: Expr) extends UnaryExpr

object UnaryExpr {
  def dotify(o: UnaryExpr, currAvail: Int) = {
    val opStr = o match {
      case Not(_) => "!"
      case Neg(_) => "-"
    }
    val (arg, _) = o.v.dotify(currAvail)
    (opStr + arg, currAvail + 1)
  }
  def transform(o: UnaryExpr, f: PartialFunction[Expr, Expr]) = {
    o match {
      case Not(v) => Not(v.transform(f)).transform0(f)
      case Neg(v) => Neg(v.transform(f)).transform0(f)
    }
  }
}


/**
 * Function calls
 */
trait CallExpr extends Expr
// f(e*)
case class FunCall(f: String, args: List[Expr]) extends CallExpr {
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
trait IndexOperator
// even indices 0, 2, ...
case object Even extends IndexOperator
// odd indices 1, 3, ...
case object Odd extends IndexOperator
// every-N indices
case class Every(v: Int) extends IndexOperator
// f[EVEN/ODD/EVERY N]
case class ArrayIndex(e: Expr, ix: IndexOperator) extends CallExpr {
  def dotify(currAvail: Int): (String, Int) = {
    val indexOp = ix match {
      case Even => "even"
      case Odd => "odd"
      case Every(n) => "every " + n
    }
    val arrayExpr = e.dotify(currAvail)._1
    val indexExpr =  arrayExpr + "[" + indexOp + "]"
    (indexExpr, currAvail + 1)
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
trait TableExpr extends Expr {
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


/**
 * Predicate expressions, commonly used in where-clauses for SQL
 */
trait PredicateExpr extends Expr {
  def dotify(currAvail: Int) = PredicateExpr.dotify(this, currAvail)
  def transform(f: PartialFunction[Expr, Expr]) = PredicateExpr.transform(this, f)
}

case class In(l: Expr, r: List[Expr]) extends PredicateExpr
case class Like(l: Expr, r: Expr) extends PredicateExpr
case class Is(l: Expr, r: Expr) extends PredicateExpr
case class Overlaps(l: (Expr, Expr), r: (Expr, Expr)) extends PredicateExpr
case class Between(l: Expr, r: (Expr, Expr)) extends PredicateExpr

object PredicateExpr {
  def dotify(p: PredicateExpr, currAvail: Int) = {
    val opLabel = p match {
      case In(_, _) => "in"
      case Like(_, _) => "like"
      case Is(_, _) => "is"
      case Overlaps(_, _) => "overlaps"
      case Between(_, _) => "between"
    }
    val (left, right) = p match {
      case In(l, r) => {
        val (left, _) = l.dotify(currAvail)
        val right = "(" + r.map(_.dotify(currAvail)._1).mkString(",") + ")"
        (left, right)
      }
      case Between(l, (r1, r2)) => {
        val (left, _) = l.dotify(currAvail)
        val (right1, _) = r1.dotify(currAvail)
        val (right2, _) = r2.dotify(currAvail)
        val right = "(" + right1 + "," + right2 + ")"
        (left, right)
      }
      case Overlaps((l1, l2), (r1, r2)) => {
        val (left1, _) = l1.dotify(currAvail)
        val (left2, _) = l2.dotify(currAvail)
        val (right1, _) = r1.dotify(currAvail)
        val (right2, _) = r2.dotify(currAvail)
        val left = "(" + left1 + "," + left2 + ")"
        val right = "(" + right1 + "," + right2 + ")"
        (left, right)
      }
      case Like(l, r) => {
        val (left, _) = l.dotify(currAvail)
        val (right, _) = r.dotify(currAvail)
        (left, right)
      }
      case Is(l, r) => {
        val (left, _) = l.dotify(currAvail)
        val (right, _) = r.dotify(currAvail)
        (left, right)
      }
    }
    (left + opLabel + right, currAvail + 1)
  }

  def transform(p: PredicateExpr, f: PartialFunction[Expr, Expr]) = {
    p match {
      case In(l, r) => In(l.transform(f), r.map(_.transform(f))).transform0(f)
      case Like(l, r) => Like(l.transform(f), r.transform(f)).transform0(f)
      case Is(l, r) => Is(l.transform0(f), r.transform0(f)).transform0(f)
      case Overlaps((l1, l2), (r1, r2)) => {
        val newL = (l1.transform(f), l2.transform(f))
        val newR = (r1.transform(f), r2.transform(f))
        Overlaps(newL, newR).transform0(f)
      }
      case Between(l, (r1, r2)) => Between(l.transform0(f), (r1.transform0(f), r2.transform0(f)))
    }
  }

}