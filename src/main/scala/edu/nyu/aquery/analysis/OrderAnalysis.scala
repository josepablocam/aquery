package edu.nyu.aquery.analysis

import edu.nyu.aquery.analysis.AnalysisTypes.TUnknown
import edu.nyu.aquery.ast._

import scala.annotation.tailrec

/**
 * Provides order-related analysis functions for a program.
 * @param info function information (environment) for the given program
 */
class OrderAnalysis(info: FunctionInfo) {
  /**
   * Recursively inspect sub-expressions of a list of expressions for order-dependence.
   * Order-dependence is acquired by the call to a built-in or UDF with order-dependence.
   * @param es
   * @return
   */
  @tailrec
  private def hasOrderDependence0(es: Seq[Expr]): Boolean = es match {
    case Nil => false
    case FunCall(f, args) :: xs =>
      // ugh...@tailrec complains in hasOd(info, f) || rec_call
      if (hasOrderDependence(f)) true else hasOrderDependence0(args ++ xs)
    case e :: xs => hasOrderDependence0(e.children ++ xs)
  }

  /**
   * Determine if a user-defined function should be classified as order-dependent, given
   * its body
   * @param udf user defined function
   * @return
   */
  def hasOrderDependence(udf: UDF): Boolean =
    hasOrderDependence0(udf.cs.map { case Left(Assign(_, e)) => e; case Right(e) => e })

  /**
   * Determines if an expression contains any order-dependent members.
   * @param e expression to check
   * @return true if order dependence in any subexpression
   */
  def hasOrderDependence(e: Expr): Boolean = hasOrderDependence0(List(e))

  /**
   * Determine if a function is order-dependent by looking up the name. If not found,
   * defaults to true for soundness
   * @param fname function name
   * @return
   */
  def hasOrderDependence(fname: String): Boolean = info(fname).map(_.orderDependent).getOrElse(true)

  /**
   * Determines if a user defined function removes order-dependence upon output. Removing
   * order-dependence means that the result of the function can be freely combined with non-sorted
   * arrays. Note that a udf can be order-dependent on its input and remove order-dependence. One
   * such example is f(x){ z := sums(x); max(z) }
   * if we call c1 + f(x), we need to sort x (as sums(x) is order-dependent), but we need not
   * sort c1, as the output of f is no longer order-dependent (removing order-dependence can be
   * viewed somewhat similar to sanitized outputs in taint analysis)
   * @param udf
   * @return
   */
  def removesOrderDependence(udf: UDF): Boolean = {
    // parameters themselves do not remove order dep. e.g. f(x, y){ x + y } doesn't remove order dep.
    val initLocals = udf.args.map(a => a -> false).toMap
    // analyze body of function sequentially
    // seed with true, as an empty function does indeed remove order-dependence (trivially)
    // note empty functions are ruled out by parsing however
    udf.cs.foldLeft((initLocals, true)) { case ((locals, rem), c) =>
      c match {
        case Left(Assign(v, ex)) =>
          val res = removesOrderDependence(locals, ex)
          (locals.updated(v, res), res)
        case Right(ex) => (locals, removesOrderDependence(locals, ex))
      }
    }._2
  }

  /**
   * Determines if an expression removes order dependence, given a map from local variables to
   * their own order-dependence removal status
   * @param locals map from local variables to whether or not they remove order dependence
   * @param expr
   * @return
   */
  def removesOrderDependence(locals: Map[String, Boolean], expr: Expr): Boolean =
    expr match {
      case Id(v) => locals.getOrElse(v, false)
      case FunCall(f, _) => removesOrderDependence(f)
      case lit: Lit => true
      case _ => expr.children.forall(removesOrderDependence(locals, _))
    }

  /**
   * Given a function name (UDF or built-in), determines if they remove order-dependence.
   * Note that this is false if function not found, for soundness
   * @param fname name of function
   * @return
   */
  def removesOrderDependence(fname: String): Boolean = info(fname).exists(_.remOrderDependence)

  /**
   * Collects names (columns (ids) and column accesses (table.column)) that require ordering as
   * a direct consequence of being used in a call to a function that is order-dependent.
   * Whether or not a name is collected depends on the context in which it is evaluated. For example
   * in max(c1, sums(c4)), c1 is not collected, but c4 is.
   *
   * Given an expression `e`, where a function `f` is called, we say name `n` is in `f`'s
   * order-scope if if it is in a subexpression of that call and `n` is not in the order-scope
   * of a deeper call in the subexpression tree.
   *
   * The general strategy can be summarized as follows:
   * - If a function is not order-dependent, and removes order-dependence upon output, then
   * names in its order-scope are not collected
   * - If a function is order-dependent, any name in its order-scope is collected
   * @param expr
   * @param collect
   * @return
   */
  def directOrderDependentCols(expr: Expr, collect: Boolean = false): Set[Expr] = expr match {
    case FunCall(f, args) if !hasOrderDependence(f) && removesOrderDependence(f) =>
      args.flatMap(directOrderDependentCols(_, collect = false)).toSet
    case FunCall(f, args) if hasOrderDependence(f) =>
      args.flatMap(directOrderDependentCols(_, collect = true)).toSet
    case Id(_) if collect => Set(expr)
    case ColumnAccess(_, _) if collect => Set(expr)
    case _ => expr.children.flatMap(directOrderDependentCols(_, collect)).toSet
  }


  /**
   * Collects names that interact in an expression. We say that names n1 and n2 interact if
   * they are in the same order-scope of a function call f, where f removes order dependence,
   * (with names outside of any function call said to interact by default). We represent
   * interactions by a set of sets of expressions, with each set representing one interaction
   * group.
   *
   * For example, given
   * c1 + c2 + max(c3 + sums(c4), c5) => Set(c1, c2), Set(c3, c4, c5)
   * max(c3, sum(c4)) => Set(c3), Set(c4)
   *
   * @param expr
   * @return
   */
  def collectInteractions(expr: Expr): Set[Set[Expr]] = {
    // simple helper to combine a sequence of sets of expressions
    def combine(s: Seq[Set[Expr]]): Set[Expr] = s.foldLeft(Set.empty[Expr])(_ ++ _)
    // collect interactions
    def collect(info: FunctionInfo, expr: Expr): (Set[Expr], Seq[Set[Expr]]) = expr match {
      // if call removes order, then we're done collecting current order-scope
      case FunCall(f, args) if removesOrderDependence(f) =>
        val (curr, acc) = args.map(e => collect(info, e)).unzip
        (Set(), combine(curr) +: acc.flatten)
      case Id(_) => (Set(expr), Nil)
      case ColumnAccess(_, _) => (Set(expr), Nil)
      case _ =>
        val (curr, acc) = expr.children.map(e => collect(info, e)).unzip
        (combine(curr), acc.flatten)
    }
    val (last, acc) = collect(info, expr)
    (if (last.isEmpty) acc else last +: acc).toSet
  }

  /**
   * Given interactions and a seed set, extend the seed set by adding sets from interactions
   * that have an element from the seed set. This extension is performed until a fixed point is
   * reached. So this is effectively the transitive closure of the `contains` relation.
   *
   * Returns the elements that were not in the original seed set.
   *
   * From an order-dependence perspective, if `interactions` are interactions in an expression
   * and `init` are the directly order dependent columns, the return set represents columns
   * that are indirectly order-dependent.
   * 
   * For example in
   * c4 + sums(c3) * max(c2, c3), we need to sort c3 (in order-scope of sums), c2 and c4.
   * We sort c2 as it interacts with c3, and similarly we sort c4 as it interacts with the call
   * to sums
   * @param interactions set of interactions
   * @param init intial seed set, in order-dependence context: directly order dependent cols
   * @return
   */
  def indirectReachable(interactions: Set[Set[Expr]], init: Set[Expr]): Set[Expr] = {
    // for interaction purposes, assume t.c1 = c1, as this is the most conservative choice
    def colEquals(c1: Expr, c2: Expr): Boolean = (c1, c2) match {
      case (Id(n1), ColumnAccess(_, n2)) => n1 == n2
      case (ColumnAccess(_, n1), Id(n2)) => n1 == n2
      case (x, y) => x == y
    }
    @tailrec
    // transitive closure
    def loop(interactions: Set[Set[Expr]], needOrder: Set[Expr]): Set[Expr] = {
      // if a set contains any element from needOrder, then we want to add the whole set
      val add = interactions.filter { inter =>
        // needOrder contains that column (exactly) or it is equal under our equality measure
        inter.exists(c1 =>
          needOrder.contains(c1) || needOrder.exists(colEquals(c1, _))
        )
      }
      val flatAdd = add.foldLeft(Set.empty[Expr])(_ ++ _)
      // remove the sets we added and extend the order set
      if (add.isEmpty) needOrder else loop(interactions -- add, flatAdd ++ needOrder)
    }
    // the difference between the final needOrder and the original are the indirect elements
    // added through the closure
    loop(interactions, init) -- init
  }


  /**
   * Given a sequence of expressions, obtain all columns that must be sorted to maintain
   * the semantics of the expression, if we decide to sort only the minimal necessary columns.
   * @param exprs expressions
   * @param collectAtRoot if true, collects names that appear outside of any order-scope
   *                      For example, in projection this would be true, while in filter it
   *                      would be false
   *                      SELECT c1 from ..., we want c1 to be sorted
   *                      SELECT .. FROM t WHERE c1 > 2, we don't care if c1 is sorted
   *                      (assuming all else equal)
   * @return set of names to be sorted (columns, as Ids or ColumnAccess)
   */
  def colsToSort(exprs: Seq[Expr], collectAtRoot: Boolean = false): Set[Expr] = {
    val direct = exprs.flatMap(directOrderDependentCols(_, collect = collectAtRoot)).toSet
    val interactions = exprs.flatMap(collectInteractions).toSet
    // indirectly order-dependent, based on direct dependence and interactions in expressions
    val indirect = indirectReachable(interactions, direct)
    direct ++ indirect
  }


  /**
   * Check if `short` is a prefix of `long`. The check for order is equality, the check for
   * column takes a function to account for situations where we might want to consider
   * things like `c1` and `t.c1` equal.
   *
   * Note that when determining what columns need to be sorted, assuming `c1 = t.c1` is a
   * conservative choice. In contrast, when determining if an ordering is a prefix, assuming this
   * same equality, in the presence of joins, can hide errors that would otherwise arise at runtime.
   * For example, consider isPrefixSort((Asc, c1), (Asc, t.c1)) in a query where there is join t, t1,
   * where t has no c1 column, but t1 does, if we assume the above equality, and
   * in doing so allow some transformation to remove the t.c1, then a runtime error that would
   * have otherwise arisen will no longer occur.
   * @param short shorter sequence of order/column pairs
   * @param long longer sequence of order/column pairs
   * @param colEquals function to check for equality of columns
   * @return true iff short is a prefix of long
   */
  def isPrefixSort(
   short: Seq[(OrderDirection, Expr)],
   long: Seq[(OrderDirection, Expr)],
   colEquals: (Expr, Expr) => Boolean)
  : Boolean =
    short.length <= long.length && short.zip(long).forall { case ((d1, c1), (d2, c2)) =>
      d1 == d2 && colEquals(c1, c2)
    }
}

object OrderAnalysis {
  /**
   * Collect an environment with order-dependence information for UDFs in a program and use
   * it to initialize an OrderAnalysis instance
   * @param prog program to analyze
   * @return OrderAnalysis instance that can be used to analyze the given program
   */
  def apply(prog: Seq[TopLevel]): OrderAnalysis = {
    val getOrderInfo = (s: FunctionInfo, f: UDF) => {
      val analyzer = new OrderAnalysis(s)
      val orderDependent = analyzer.hasOrderDependence(f)
      val remsOrderDependence = analyzer.removesOrderDependence(f)
      s.write(f.n, new UDFSummary(f.n, { case _ => TUnknown }, orderDependent, remsOrderDependence))
    }
    // environment is collected sequentially
    val funs = prog.collect { case f: UDF => f }
    new OrderAnalysis(FunctionInfo(funs, getOrderInfo))
  }

  /**
   * Instance of OrderAnalysis with only built-in function info
   * @return
   */
  def apply(): OrderAnalysis = apply(Nil)
}
