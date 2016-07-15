package edu.nyu.aquery.ast

/**
 * Optimizable components are encoded using operators that include a value field `attr` which is a
 * mapping to any possible attributes that an analyzer might need/use. We encode this as a map to
 * allow extension with attributes that might not be currently considered.
 */
trait Analyzable[T <: Analyzable[T]] {
  self: T =>
  /**
   * Contains properties assigned/used by analyzer
   */
  val attr: Map[Any, Any]

  /**
   * Convenience wrapper to create updated copy with new key/value pair
   * @param k key
   * @param v value
   */
  def setAttr(k: Any, v: Any): T
}

/**
 * All optimizable query componentats are from relation algebra and they are allowed
 * as top-level constructs in AQuery
 */
trait RelAlg extends AST[RelAlg] with Analyzable[RelAlg] with TopLevel {
  def expr: Seq[Expr]
  def children: Seq[RelAlg]
  def dotify(currAvail: Int): (String, Int) = RelAlg.dotify(this, currAvail)
  def transform(f: PartialFunction[RelAlg, RelAlg]) = RelAlg.transform(this, f)
  def setAttr(k: Any, v: Any): RelAlg
}

/**
 * Encodes a query
 * @param local list of local queries, triple of name, column names (maybe empty) and the
 *              query encoded using relational algebra
 * @param main main query, encoded using relational algebra
 * @param attr
 */
case class Query(
  local: List[(String, List[String], RelAlg)],
  main: RelAlg,
  attr: Map[Any, Any] = Map()) extends AST[Query] with Analyzable[Query] with TopLevel {
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))

  def dotify(currAvail: Int) = {
    val selfNode = Dot.declareNode(currAvail, "full-query")
    val (localNodes, mainId) = Dot.strList(
      local,
      currAvail + 1,
      (e:(String, List[String], RelAlg), id: Int) => {
        val nameLabel = e._1 + "(" + e._2.mkString(",") + ")"
        val nameNode = Dot.declareNode(id, nameLabel)
        val nameEdge = Dot.declareEdge(currAvail, id)
        val (queryNode, nextAvail) = e._3.dotify(id + 1)
        val queryEdge = Dot.declareEdge(id, id + 1)
        (nameNode + nameEdge + queryNode + queryEdge, nextAvail)
      }
    )
    val (mainNode, nextAvail) = main.dotify(mainId)
    val mainEdge = Dot.declareEdge(currAvail, mainId)
    (selfNode + localNodes + mainEdge + mainNode, nextAvail)
  }

  def transform(f: PartialFunction[Query, Query]) = Query(local, main, attr).transform0(f)

  def transformQueries(f: PartialFunction[RelAlg, RelAlg]) = {
    val newLocal = local.map { case (n, cs, q) => (n, cs, q.transform(f))}
    val newMain = main.transform(f)
    Query(newLocal, newMain, attr)
  }
}

/**
 * Projection operator
 * @param t relation to project
 * @param ps projections as tuples of expression and optional name
 * @param attr
 */
case class Project(
  t: RelAlg,
  ps: List[(Expr, Option[String])],
  attr: Map[Any, Any] = Map()) extends RelAlg {
  val children = List(t)
  val expr = ps.map(_._1)
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
}

/**
 * Filter operator
 * @param t relation to filter
 * @param fs list of expressions used to filter
 * @param attr
 */
case class Filter(
  t: RelAlg,
  fs: List[Expr],
  attr: Map[Any, Any] = Map()) extends RelAlg {
  val children = List(t)
  val expr = fs
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
}

/**
 * Grouping operator
 * @param t relation to group
 * @param gs list of grouping expressions with optional names
 * @param having optional list of "having" filters
 * @param attr
 */
case class GroupBy(
  t: RelAlg,
  gs: List[(Expr, Option[String])],
  having: List[Expr],
  attr: Map[Any, Any] = Map()) extends RelAlg {
  val children = List(t)
  val expr = gs.map(_._1) ++ having
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
}

/**
 * Assuming order operator (from AQuery)
 * @param t relation to sort
 * @param os order of sorting encode as a tuples of sorting direction and field name
 * @param attr
 */
case class SortBy(
  t: RelAlg,
  os: List[(OrderDirection, Expr)],
  attr: Map[Any, Any] = Map()) extends RelAlg {
  val children = List(t)
  val expr: Seq[Expr] = os.map(_._2)
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
}

// Possible directions for sorting
trait OrderDirection
case object Asc extends OrderDirection
case object Desc extends OrderDirection


// Possible types of join
trait JoinType
case object InnerJoinUsing extends JoinType
case object FullOuterJoinUsing extends JoinType
case object InnerJoinOn extends JoinType
case object Cross extends JoinType


/**
 * A simple wrapper around a table name
 * @param n table name
 * @param alias optional alias
 * @param attr
 */
case class Table(
  n: String,
  alias: Option[String] = None,
  attr: Map[Any, Any] = Map()) extends RelAlg {
  val children: Seq[RelAlg] = Nil
  val expr: Seq[Expr] = Nil
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
}

/**
 * Applying a single-argument function to an existing relation/query at the top of a query
 * e.g. SHOW(SELECT * FROM * ...)
 * @param f function name as string
 * @param t table/query encoded as a relational algebra operator
 * @param attr
 */
case class TopApply(
  f: String,
  t: RelAlg,
  attr: Map[Any, Any] = Map()) extends RelAlg {
  val children = List(t)
  val expr: Seq[Expr] = Nil
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
}

/**
 * Applying a function to a set of argument expressions at the bottom of a query (i.e. as
 * a source in the from clause).
 * e.g. SELECT * FROM g(t, 2, 3) ...
 * @param f function name as string
 * @param args list of argument expressions
 * @param attr
 */
case class BottomApply(
  f: String,
  args: List[Expr],
  attr: Map[Any, Any] = Map()) extends RelAlg {
  val children: Seq[RelAlg] = Nil
  val expr = args
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
}

/**
 * Joining of 2 relations
 * @param jt join type
 * @param l lhs argument
 * @param r rhs argument
 * @param cond list of expression used in join. If join USING this is a list of colum names,
 *             otherwise it is a list of predicate expressions
 * @param attr
 */
case class Join(
  jt: JoinType,
  l: RelAlg,
  r: RelAlg,
  cond: List[Expr],
  attr: Map[Any, Any] = Map()) extends RelAlg {
  val children = List(l, r)
  val expr = cond
  def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
}


object RelAlg {
  def dotify(o: RelAlg, currAvail: Int) = o match {
    case Project(t, ps, _) => {
      val projectionsLabel = "project " + ps.map {
        case (e, Some(n)) => e.dotify(currAvail)._1 + " as " + n
        case (e, None) => e.dotify(currAvail)._1
      }.mkString(" ,")
      val projectionsNode = Dot.declareNode(currAvail, projectionsLabel)
      val sourceEdge = Dot.declareEdge(currAvail, currAvail + 1)
      val (sourceNode, nextAvail) = t.dotify(currAvail + 1)
      (projectionsNode + sourceEdge + sourceNode, nextAvail)
    }
    case Filter(t, fs, _) => {
      val filtersLabel = "where " + fs.map(_.dotify(currAvail)._1).mkString(", ")
      val filtersNode = Dot.declareNode(currAvail, filtersLabel)
      val sourceEdge = Dot.declareEdge(currAvail, currAvail + 1)
      val (sourceNode, nextAvail) = t.dotify(currAvail + 1)
      (filtersNode + sourceEdge + sourceNode, nextAvail)
    }
    case GroupBy(t, gs, having, _) => {
      val groupsLabel = gs.map {
        case (e, Some(n)) => e.dotify(currAvail)._1 + " as " + n
        case (e, None) => e.dotify(currAvail)._1
      }.mkString(" ,")
      val havingLabel = having.map(_.dotify(currAvail)._1).mkString(", ")
      val fullLabel = "group-by " + groupsLabel + (if (having.nonEmpty) " having " + havingLabel else "")
      val groupNode = Dot.declareNode(currAvail, fullLabel)
      val sourceEdge = Dot.declareEdge(currAvail, currAvail + 1)
      val (sourceNode, nextAvail) = t.dotify(currAvail + 1)
      (groupNode + sourceEdge + sourceNode, nextAvail)
    }
    case SortBy(t, os, _) => {
      val sortLabel = "sort: " + os.map(x => x._1 + " " + x._2.dotify(1)._1).mkString(", ")
      val sortNode = Dot.declareNode(currAvail, sortLabel)
      val sourceEdge = Dot.declareEdge(currAvail, currAvail + 1)
      val (sourceNode, nextAvail) = t.dotify(currAvail + 1)
      (sortNode + sourceEdge + sourceNode, nextAvail)
    }
    case Table(n, alias, _) => {
      val label = n + (alias match {
        case Some(a) => " as " + a
        case None => ""
      })
      (Dot.declareNode(currAvail, label), currAvail + 1)
    }
    case TopApply(fun, t, _) => {
      val funNode = Dot.declareNode(currAvail, fun)
      val funEdge = Dot.declareEdge(currAvail, currAvail + 1)
      val (srcNode, nextAvail) = t.dotify(currAvail + 1)
      (funNode + funEdge + srcNode, nextAvail)
    }
    case BottomApply(f, args, _) => {
      val label = f + "(" + args.map(_.dotify(currAvail)._1).mkString(", ") + ")"
      (Dot.declareNode(currAvail, label), currAvail + 1)
    }
    case Join(jt, l, r, cond, _) => {
      val joinNode = Dot.declareNode(currAvail, jt.toString)
      val leftEdge = Dot.declareEdge(currAvail, currAvail + 1)
      val (leftNode, rightId) = l.dotify(currAvail + 1)
      val rightEdge = Dot.declareEdge(currAvail, rightId)
      val (rightNode, condId) = r.dotify(rightId)
      val condEdge = Dot.declareEdge(currAvail, condId)
      val condLabel = "conds: " + cond.map(_.dotify(condId)._1).mkString(", ")
      val condNode = Dot.declareNode(condId, condLabel)
      (joinNode + leftEdge + leftNode + rightEdge + rightNode + condEdge + condNode, condId + 1)
    }
  }

  def transform(o: RelAlg, f: PartialFunction[RelAlg, RelAlg]): RelAlg = o match {
    // operators
    case Project(t, ps, attr) => Project(t.transform(f), ps, attr).transform0(f)
    case Filter(t, fs, attr) => Filter(t.transform(f), fs, attr).transform0(f)
    case GroupBy(t, gs, having, attr) => GroupBy(t.transform(f), gs, having, attr).transform0(f)
    case SortBy(t, os, attr) => SortBy(t.transform(f), os, attr).transform0(f)
    // relation sources
    case Table(n, alias, attr) => Table(n, alias, attr).transform0(f)
    case TopApply(fun, t, attr) => TopApply(fun, t.transform(f), attr).transform0(f)
    case BottomApply(fun, args, attr) => BottomApply(fun, args, attr).transform0(f)
    case Join(jt, l, r, cond, attr) => Join(jt, l.transform(f), r.transform(f), cond, attr).transform0(f)
  }
}



/**
 * Encodes any "query"-type operations that perform modifications on data. This includes
 * update and delete statements. These are allowed as top-level constructs.
 */
trait ModificationQuery extends AST[ModificationQuery] with TopLevel {
  def dotify(currAvail: Int) = ModificationQuery.dotify(this, currAvail)
  // We don' optimize updates/deletes
  def transform(f: PartialFunction[ModificationQuery, ModificationQuery]) = this
  def expr: Seq[Expr]
}

/**
 * Update statement
 * @param t relation to update
 * @param assigns tuples of field name and expression to assign to it
 * @param order possible sorting of data prior to updating
 * @param where constraints for rows to be updated
 * @param groupby groupby clause
 * @param having possible having clause. It is not legal to have a having clause without a grouping
 */
case class Update(
  t: String,
  assigns: List[(String, Expr)],
  order: List[(OrderDirection, Expr)],
  where: List[Expr],
  groupby: List[Expr],
  having: List[Expr]) extends ModificationQuery {
  val expr = assigns.map(_._2) ++ order.map(_._2) ++ where ++ groupby ++ having
}

/**
 * Delete statements
 * @param t relation from which to delete
 * @param del columns to delete or constraints for rows to delete
 * @param order assumes order prior to deletions
 * @param groupby possible group by clause
 * @param having possible having clause, requires a group-by clause
 */
case class Delete(
  t: String,
  del: Either[List[String], List[Expr]],
  order: List[(OrderDirection, Expr)],
  groupby: List[Expr],
  having: List[Expr]) extends ModificationQuery {
  val expr =
    (del match { case Left(_) => Nil; case Right(es) => es}) ++ order.map(_._2) ++ groupby ++ having
}

object ModificationQuery {
  def dotify(o: ModificationQuery, currAvail: Int) = {
    val nodeLabel = o match {
      case Update(t, _, _, _, _, _) => "update on " + t
      case Delete(t, _, _, _, _) => "delete on " + t
    }
    (Dot.declareNode(currAvail, nodeLabel), currAvail + 1)
  }
}