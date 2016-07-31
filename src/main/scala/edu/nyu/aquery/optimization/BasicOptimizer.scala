package edu.nyu.aquery.optimization

import edu.nyu.aquery.analysis.{JoinAnalysis, OrderAnalysis}
import edu.nyu.aquery.ast._

/**
 * Basic optimizer, implementing order-based rewrites and some traditional optimizations. Given a
 * program input and a list of requested optimizations, can optimize the given program. Note that
 * the analysis/optimization is program-specific, so each program optimized should have its own
 * Optimizer instance
 * @param input input program
 * @param optims
 */
class BasicOptimizer(val input: Seq[TopLevel] = Nil, val optims: Seq[String] = Nil) extends Optimizer {
  import BasicOptimizer._

  val description =
    """
      Basic optimizer.
      Optimizations available:
      -------------------------
      simplifySort -> remove sort based on context and order of tables used
      filterBeforeSort -> push selections below a sort
      embedSort -> embed a sort in a group-by clause
      simplifyEmbeddedSort -> simplify an embedded sort based on group-by implicit order
      pushFiltersJoin -> push selections below joins
      makeReorderFilter -> create filter nodes with movable selections (keeps semantics)
      sortToSortCols -> sort only needed columns
    """

  val orderAnalyzer: OrderAnalysis = OrderAnalysis(input)

  /**
   * Ordered list of optimizations available for application. Note that the order is relevant,
   * as some of the later transformations replace some nodes with different node types.
   * Transformations in lower indices should be applied first
   */
  val optimsAvailable: List[(String, optimization)] =
    List(
      "simplifySort" -> ((e, q) => simplifySort(e, q)),
      "filterBeforeSort" -> ((e, q) => filterBeforeSort(q)),
      "embedSort" -> ((e, q) => embedSort(q)),
      "simplifyEmbeddedSort" -> ((e, q) => simplifyEmbeddedSort (q)),
      "pushFiltersJoin" -> ((e, q) => pushFiltersJoin(q)),
      "makeReorderFilter" -> ((e, q) => makeReorderFilter(q)),
      "sortToSortCols" -> ((e, q) => sortToSortCols(q))
    )

  /**
   * Given a set of requested optimizations, combine them into a single optimization function
   * (batch)
   * @param opts
   * @return
   */
  def makeOptimizationBatch(opts: Seq[String]): optimization = {
    val unknownOpts = opts.filterNot(optimsAvailable.map(_._1).contains)
    // validate optimizations requested
    if (unknownOpts.nonEmpty)
      throw new Exception("Unknown optimizations: " + unknownOpts.mkString(","))
    val requested = optimsAvailable.collect { case (n, f) if opts.contains(n) => f }
    val clean = if (requested.isEmpty) optimsAvailable.map(_._2) else requested
    // Note that order is relevant, so foldLeft guarantees we apply transformations
    // earlier in the `optimsAvailable` list
    (env, query) => clean.foldLeft(query)((q, f) => f(env, q))
  }

  // A function to optimize a query plan
  val optimizePlan = makeOptimizationBatch(optims)

  /**
   * Apply a filter before a sort-by, by extracting all selections up to first
   * order-dependent selection. Selections prior to that can be applied before sort
   * and are thus pushed below, while all remaining selections are applied after the sort
   * @param r
   * @return
   */
  def filterBeforeSort(r: RelAlg): RelAlg = {
    val optim: PartialFunction[RelAlg, RelAlg] = {
      case orig @ Filter(SortBy(src, order, _), fs, _) =>
        // expressions with no order dependencies are safe to execute before sort
        val (before, after) = fs.span(!orderAnalyzer.hasOrderDependence(_))
        // need sort before anything can be filtered
        if (before.isEmpty)
          orig
        // we can push entire filter down
        else if (after.isEmpty)
          SortBy(Filter(src, before), order)
        // some selections can be applied before and after
        else
          Filter(SortBy(Filter(src, before), order), after)
    }
    r.transform(optim)
  }

  /**
   * Embed a sort within a group-by (making it a sort-each effectively) if there
   * are not order-dependent expressions in the group-by or having expressions
   * @param r
   * @return
   */
  def embedSort(r: RelAlg): RelAlg = {
    val optim: PartialFunction[RelAlg, RelAlg] = {
      case orig @ GroupBy(SortBy(src, order, _), groups, having, attr) =>
        // if none of the expressions depend on order, we can group first
        if (!orig.expr.exists(orderAnalyzer.hasOrderDependence))
          SortBy(GroupBy(src, groups, having, attr), order)
        else
          orig
    }
    r.transform(optim)
  }


  /**
   * For purposes of prefixes, if there is a join, we need to be conservative and require exact
   * matches (otherwise removals based on this criteria could omit runtime errors that should
   * otherwise occur). When there is no join, then we can consider _.c1 and c1 the same
   * @param x
   * @param y
   * @param hasJoin
   * @return
   */
  private def prefixColEquals(x: Expr, y: Expr, hasJoin: Boolean): Boolean = (x, y) match {
    case (Id(c1), ColumnAccess(_, c2)) if !hasJoin => c1 == c2
    case (ColumnAccess(_, c2), Id(c1)) if !hasJoin => c1 == c2
    case _ => x == y
  }

  /**
   * Simplify a embedded sort, by accounting for possible prefix sort generated by group-by
   * expressions. Prefix sort can be generated in group-by if a grouping expression is
   * of the form column | table.column. The prefix sort is solely ascending in each dimensions.
   * Finally, note that given that we cannot resolve whether two columns are equivalent
   * (e.g. c1 and t.c1) in the presence of multiple tables (i.e. with a join), the group
   * expression must match (exactly) the stated order (so ASC c1 and GROUP BY t.c1 won't be
   * considered matches)
   * @param r
   * @return
   */
  def simplifyEmbeddedSort(r: RelAlg): RelAlg = {
    val optim: PartialFunction[RelAlg, RelAlg] = {
      case orig @ SortBy(GroupBy(src, groups, h, a), order, _) =>
        val simpleGroups = groups.map(_._1).forall {
          case Id(_) | ColumnAccess(_, _) => true
          case _ => false
        }
        // if !hasJoin(src) then c1 and t.c1 should be ok and considered the same
        // so use appropriate when checking for prefix
        val hasJoin = JoinAnalysis.hasJoin(src)
        // note that groups sort ascending by default
        val prefix = orderAnalyzer.isPrefixSort(
          groups.map(x => (Asc, x._1)), order, prefixColEquals(_, _, hasJoin)
        )

        // if doesn't satisfy constraints, just return orig
        if(!simpleGroups || !prefix)
          orig
        else if (order.length == groups.length)
        // we could eliminate sort completely
          GroupBy(src, groups, h, a)
        else
        // drop matched portions of sort
          SortBy(GroupBy(src, groups, h, a), order.drop(groups.length))
    }
    r.transform(optim)
  }

  /**
   * From a list of expression create a list of list of expressions, where expressions in each list
   * (functioning as selections) can be reordered but maintaining the overall semantics the same.
   * This requires cutting along lines of expressions involving aggregates vs those that do not
   * @param exprs
   * @return
   */
  def cutFilters(exprs: Seq[Expr]): Seq[Seq[Expr]] = {
    def loop(left: Seq[Expr], curr: Seq[Expr], acc: Seq[Seq[Expr]]): Seq[Seq[Expr]] = left match {
      case Nil => if (curr.nonEmpty) curr.reverse +: acc else acc
      // not movable, should be its own list, reverse curr to make order of filters correct
      case x :: xs if orderAnalyzer.usesAggregate(x) =>
        loop(xs, Nil, List(x) +: (if (curr.nonEmpty) curr.reverse +: acc else acc))
      // can continue to accumulate in current unit
      case x :: xs if !orderAnalyzer.usesAggregate(x) => loop(xs, x +: curr, acc)
    }
    loop(exprs, Nil, Nil).reverse
  }

  /**
   * Cut up a normal filter into "reorder" filters, in which selections can be moved around
   * freely without affecting semantics. Given that this creates a new filter type, it should
   * be applied last (after all other transformations that pattern match on Filter, are done.
   * @param r
   * @return
   */
  def makeReorderFilter(r: RelAlg): RelAlg = {
    val optim: PartialFunction[RelAlg, RelAlg] = {
      case Filter(src, fs, attr) =>
        // copy over attributes as well
        cutFilters(fs).foldLeft(src) { case (s, f) => ReorderFilter(s, f.toList, attr) }
    }
    r.transform(optim)
  }

  /**
   * Remove/simplify order in a simple from-clause based on existing order for temporary tables.
   * If the existing order is a prefix of the desired order, can simplify, if exact match,
   * can remove order, if neither, returns query unchanged.
   * @param env map from table name to existing order
   * @param r query
   * @return
   */
  def simplifySort(env: orderEnv, r: RelAlg): RelAlg = {
    val optim: PartialFunction[RelAlg, RelAlg] = {
      case orig @ SortBy(t @ Table(n, _, _), want, _) =>
        env.get(n).map { has =>
          val isPrefix = orderAnalyzer.isPrefixSort(has, want, prefixColEquals(_, _, hasJoin = false))
          if (!isPrefix)
            orig
          else if (has.length == want.length)
            t
          else
            SortBy(t, want.drop(has.length))
        }.getOrElse(orig)
    }
    r.transform(optim)
  }

  /**
   * Push filters below a join, based on the tables needed and whether or not the filters
   * use aggregates. Filters that use aggregates can have different values once join takes place
   * so we shouldn't push
   * @param r
   * @return
   */
  def pushFiltersJoin(r: RelAlg): RelAlg = {
    val optim: PartialFunction[RelAlg, RelAlg] = {
      case Filter(j @ Join(_, _, _, _, _), fs, _) =>
        val (before, after) = fs.span(!orderAnalyzer.usesAggregate(_))
        // we push down filters that can be executed "before"
        // note foldRight, each filter is placed as deeply as possible
        // so for earlier filters to execute first, they must be push down later in fold
        // hence foldRight
        val filteredJoin = before.foldRight(j: RelAlg)((f, acc) =>  JoinAnalysis.placeFilter(acc, f))
        if (after.isEmpty) filteredJoin else Filter(filteredJoin, after)
    }
    r.transform(optim)
  }

  /**
   * Count number of sort-by nodes in a query plan
   * @param r
   * @return
   */
  def countSortBy(r: RelAlg): Int = r match {
    case SortBy(_, _, _) => 1
      // note that sum of empty list is 0
    case _ => r.children.map(countSortBy).sum
  }

  /**
   * Convert a standard sort-by node to a sort in which only specified columns are sorted
   * @param r
   * @return
   */
  def sortToSortCols(r: RelAlg): RelAlg = {
    // retrieve columns that should be sorted
    def getCols(p: RelAlg): Set[Expr] = {
      // all nodes in the tree above the sort by
      val nodesAbove = r.takeWhile({ case SortBy(_, _, _) => false; case _ => true}, identity)
      val mustSortAll = nodesAbove.exists { e =>
        // there is still filtering above or grouping
        e.isInstanceOf[FiltersData] || e.isInstanceOf[GroupBy]
      }
      val exprsAbove = nodesAbove.flatMap(_.expr)
      if (mustSortAll)
        orderAnalyzer.allCols(exprsAbove)
      else
        orderAnalyzer.colsToSort(exprsAbove, collectAtRoot = true)
    }

    if (countSortBy(r) > 1)
      throw new Exception("Current optimizer assumes max 1 sort-by in a plan")

    if (!orderAnalyzer.hasSortBy(r))
      r
    else {
      val cols = getCols(r)
      val cleanCols: Set[Expr] = if (cols.contains(WildCard)) Set(WildCard) else cols
      val optim: PartialFunction[RelAlg, RelAlg] = {
        case s @ SortBy(src, order, _) =>
          if (cleanCols.nonEmpty)
            ColSortBy(src, order, cleanCols)
          else
            src
      }
      r.transform(optim)
    }
  }

  // TODO: join: cross to join

  def optimizeQuery(q: Query): Query = {
    // environment is full-query specific
    val initEnv: orderEnv = Map()
    val init = (initEnv, List.empty[(String, List[String], RelAlg)])
    // unlikely to have large number intermediate queries, so foldRight should be safe
    val (env, optLocal) = q.local.foldRight(init) { case ((n, cs, p), (e, qs)) =>
      // optimize local query plan
      val opt = optimizePlan(e, p)
      // add local query table order information to environment
      val order = orderAnalyzer.getSort(p)
      val newEnv = e + (n -> order)
      // updated environment and optimized query
      (newEnv, (n, cs, opt) :: qs)
    }
    Query(optLocal, optimizePlan(env, q.main), q.attr)
  }

  /**
   * Optimization routine
   * @return
   */
  def optimize: Seq[TopLevel] = input.map {
    case q @ Query(_, _, _) => optimizeQuery(q)
    case Create(n, Right(q)) =>  Create(n, Right(optimizeQuery(q)))
    case x => x
  }
}

object BasicOptimizer {
  type orderEnv = Map[String, Seq[(OrderDirection, Expr)]]
  type optimization = (orderEnv, RelAlg) => RelAlg

  /**
   * Filter in which the code generator is free to reorder selection expressions while
   * maintaining semantics
   * @param t
   * @param fs
   * @param attr
   */
  case class ReorderFilter(t: RelAlg, fs: Seq[Expr], attr: Map[Any, Any] = Map())
    extends RelAlg with FiltersData
  {
    val children = List(t)
    val expr = fs
    def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
    def clearAttr = this.copy(attr = Map())
    override def transform(f: PartialFunction[RelAlg, RelAlg]) =
      ReorderFilter(t.transform(f), fs, attr).transform0(f)
    override def dotify(currAvail: Int) = Filter(t, fs.toList, attr).dotify(currAvail)
  }

  /**
   * Sort-by in which only specified columns are sorted
   * @param t
   * @param os
   * @param cols
   * @param attr
   */
  case class ColSortBy(
    t: RelAlg,
    os: List[(OrderDirection, Expr)],
    cols: Set[Expr],
    attr: Map[Any, Any] = Map()) extends RelAlg {
    val children = List(t)
    val expr = cols.toList
    def setAttr(k: Any, v: Any) = this.copy(attr = attr.updated(k, v))
    def clearAttr = this.copy(attr = Map())
    override def transform(f: PartialFunction[RelAlg, RelAlg]) =
      ColSortBy(t.transform(f), os, cols, attr).transform0(f)
    override def dotify(currAvail: Int) = {
      val sortLabel = "sort: " + os.map(x => x._1 + " " + x._2.dotify(1)._1).mkString(", ")
      val sortCols = "cols: " + cols.map(_.dotify(1)._1).mkString(",")
      val sortNode = Dot.declareNode(currAvail, sortLabel + " for " + sortCols)
      val sourceEdge = Dot.declareEdge(currAvail, currAvail + 1)
      val (sourceNode, nextAvail) = t.dotify(currAvail + 1)
      (sortNode + sourceEdge + sourceNode, nextAvail)
    }
  }

  def apply(prog: Seq[TopLevel], opts: Seq[String] = Nil): BasicOptimizer = new BasicOptimizer(prog, opts)
  def apply(): BasicOptimizer = apply(Nil)
}
