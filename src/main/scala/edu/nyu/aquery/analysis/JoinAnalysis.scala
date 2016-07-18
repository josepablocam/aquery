package edu.nyu.aquery.analysis
import edu.nyu.aquery.ast._

object JoinAnalysis {
  // key to insert into attributes set of table names available at given node
  private val AVAIL_KEY = "TABLES_AVAIL"
  // set table names available at given node
  def setAvailTableNames(e: RelAlg, s: Set[String]): RelAlg = e.setAttr(AVAIL_KEY, s)
  // get table names available at given node
  def getAvailTableNames(e: RelAlg): Set[String] =
    e.attr.getOrElse(AVAIL_KEY, Set()).asInstanceOf[Set[String]]

  /**
   * Get tables used. For columns that are not accessed explicitly using the table name
   * (i.e. simple Id), we return None, as we cannot determine before runtime what table that
   * corresponds to. For other cases, we return Some(Set(tables))
   * @param expr
   * @return
   */
  def tableNamesUsed(expr: Expr): Option[Set[String]] = expr match {
    case ColumnAccess(t, _) => Some(Set(t))
    case Id(_) => None
    case _ =>
      val empty: Option[Set[String]] = Some(Set())
      expr.children.map(tableNamesUsed).foldLeft(empty) {
       case (_, None) | (None, _) => None
       case (Some(e), Some(acc)) => Some(e ++ acc)
     }
  }

  /**
   * Given a current relational algebra operation, what tables are available (including deeper in
   * the operation)
   * @param relAlg
   * @return
   */
  def tablesAvailable(relAlg: RelAlg): Set[Table] = relAlg match {
    case Join(_, l, r, _, _) => tablesAvailable(l) ++ tablesAvailable(r)
    case t @ Table(_, _, _) => Set(t)
    case _ => relAlg.children.map(tablesAvailable).foldLeft(Set.empty[Table])(_ ++ _)
  }

  /**
   * Determine if the top-most operation in an expression is a check for equality/disequality.
   * We also accept NOT(isEqualitySelection) and !(isEqualitySelection) as such an operation,
   * recursively checked.
   * @param expr
   * @return
   */
  def isEqualitySelection(expr: Expr): Boolean = expr match {
    case FunCall(f, arg) if
        f.toUpperCase == "NOT" &&
        arg.length == 1 &&
        isEqualitySelection(arg.head) => true
    case UnExpr(Not, arg) if isEqualitySelection(arg) => true
    case BinExpr(Eq | Neq, _, _) => true
    case _ => false
  }

  /**
   * Returns true if there is a join (including cartesian product) anywhere in the rel algebra op
   * @param relAlg
   * @return
   */
  def hasJoin(relAlg: RelAlg): Boolean = relAlg match {
    case Join(_, _, _, _,_) => true
    case _ => relAlg.children.exists(hasJoin)
  }

  /**
   * Given a table return the name that can be used to refer to that table.
   * @param t table
   * @return
   */
  def tableName(t: Table): String = t.alias.getOrElse(t.n)

  /**
   * Recursively annotate a relational algebra operation with the tables available at each node
   * @param relAlg
   * @return
   */
  def annotateWithAvailTableNames(relAlg: RelAlg): RelAlg = {
    val annotate: PartialFunction[RelAlg, RelAlg] = {
      case r =>
        val tables = r.children match {
          case c if c.isEmpty => tablesAvailable(r).map(tableName)
          // for non-base nodes just use childrens' annotations
          case xs => xs.flatMap(x => getAvailTableNames(x)).toSet
        }
        setAvailTableNames(r, tables)
    }
    // recursively annotate tree
    relAlg.transform(annotate)
  }


  /**
   * Given an expression representing a selection filter, and a relational algebra operations
   * that includes a join, place expression as deeply as possible, given the required tables.
   *
   * Note that this means that if you have expressions: e_1, e_2, e_3, and
   * you want to place these, then they must be placed by making calls in order of e_3, e_2, e_1
   * @param relAlg
   * @param filter
   * @return
   */
  def placeFilter(relAlg: RelAlg, filter: Expr): RelAlg = {
    // placement helper function. Places at deepest node in operation were all table needs are sat
    // makes sure to maintain attribute invariant
    def place: (Set[String] => PartialFunction[RelAlg, RelAlg]) = (needs: Set[String]) => {
      // simplest case: only needs this table
      case t @ Table(_, _, _) if needs.subsetOf(getAvailTableNames(t)) =>
        setAvailTableNames(Filter(t, filter :: Nil), getAvailTableNames(t))
      // this is the deepest join where needs is satisfied
      // if needs is a subset of l/r then we could place it deeper in tree
      case j @ Join(_, l, r, _, _)
        if needs.subsetOf(getAvailTableNames(j)) &&
           !needs.subsetOf(getAvailTableNames(l)) &&
           !needs.subsetOf(getAvailTableNames(r)) =>
        setAvailTableNames(Filter(j, filter :: Nil), getAvailTableNames(j))
    }

    if (!hasJoin(relAlg)) throw new Exception("Requires rel alg op with join")

    // needs to be annotated, if not, then we go ahead and annotate
    val annotated =
      if (getAvailTableNames(relAlg).isEmpty)
        annotateWithAvailTableNames(relAlg)
      else
        relAlg
    // tables needed in the selection
    val needTables = tableNamesUsed(filter)
    // all tables available in tree
    val allTables =  getAvailTableNames(annotated)
    // if needTables None (i.e. unknown), then use all tables as "needs"
    // will result in selection placed on top of last join (i.e. once guaranteed to have all data)
    needTables.map { needs =>
      annotated.transform(place(needs))
    }.getOrElse(annotated.transform(place(allTables)))
  }
}
