package edu.nyu.aquery.codegen

import edu.nyu.aquery.ast._
import edu.nyu.aquery.optimization.BasicOptimizer.{ColSortBy, ReorderFilter}

import scala.io.Source
import scala.util.Try

import scalaz.State

class KdbGenerator extends BackEnd {
  // set of built-ins (with overloads disambiguated by attaching number of args)
  // kdb versions of these are the same names just prefixed by .aq.
  val BUILT_INS =
    """
      abs, and, avg, avgs_1, avgs_2, between, concatenate, count, deltas, distinct,
      drop, list, exec_arrays, flatten, fill, first_1, first_2, is, in, indexEven, indexOdd,
      indexEveryN, last_1, last_2, like, makeNull, max, maxs_1, maxs_2, min, mins_1, mins_2,
      mod, moving, next_1, next_2, not, null, or, overlaps, pow, prev_1, prev_2, prd, prds,
      ratios, reverse, show, sum, sums_1, sums_2, sqrt, stddev, toSym, vars
    """.split(",")
      .map{w =>
        val t = w.trim
        val s = t.split("_")
        if (s.length == 1)
          (t, None)
        else
          Try((s.head, Some(s.last.toInt))).getOrElse((t, None))
      }.toSet

  // variadic functions need to be given arguments as a list, rather than in usual fashion
  // e.g (upsert/)(t;t1;t2)
  val VARIADIC = "and,concatenate,or,list".split(",").map(_.trim).toSet

  // Encapsulates the environment for purposes of code generation
  case class CodeState(
    queryId: Int = 0,           // identifier for queries (kdb functions wrap a query): .aq.qX
    tableId: Int = 0,           // identifier for temp tables: aq__tX
    currTable: String = "",     // current table name (otherwise can be taken from tableId)
    grouped: Boolean = false,   // if plan has generated code for a group-by (so must modify)
    nameAsVar: Boolean = false, // treat an Id/name as a variable name, not a column lookup
    inUDF: Boolean = false,     // code is being generated inside UDF body
    renameCols: Boolean = false // rename columns with table name prefix
    )

  // Generates code, given an environment
  type CodeGen = State[CodeState, String]
  
  // Performs manipulations (including reading) of the environment
  type CodeEnvOp[A] = State[CodeState, A]

  // need to add each modifiers when grouped
  val isGrouped: CodeEnvOp[Boolean] = SimpleState.read(_.grouped)
  val setGrouped: CodeEnvOp[Unit] = SimpleState.modify(_.copy(grouped = true))
  val remGrouped: CodeEnvOp[Unit] = SimpleState.modify(_.copy(grouped = false))
  // need to know current table
  def setCurrTable(s: String): CodeEnvOp[Unit] = SimpleState.modify(_.copy(currTable = s))
  val getCurrTable: CodeEnvOp[String] = SimpleState.read(_.currTable)
  // query identifier to generate function name
  val getQueryId: CodeEnvOp[Int] = SimpleState.read(_.queryId)
  val incrQueryId: CodeEnvOp[Unit] = SimpleState.modify(c => c.copy(queryId = c.queryId + 1))
  // table identifier to generate temp name
  val getTableId: CodeEnvOp[Int] = SimpleState.read(_.tableId)
  val incrTableId: CodeEnvOp[Unit] = SimpleState.modify(c => c.copy(tableId = c.tableId + 1))
  // avoid looking up variables as columns
  val nameAsVar: CodeEnvOp[Boolean] = SimpleState.read(_.nameAsVar)
  val setNameAsVar: CodeEnvOp[Unit] = SimpleState.modify(_.copy(nameAsVar = true))
  val remNameAsVar: CodeEnvOp[Unit] = SimpleState.modify(_.copy(nameAsVar = false))
  // need to enlist certain variable in UDF body
  val inUDF: CodeEnvOp[Boolean] = SimpleState.read(_.inUDF)
  val setInUDF: CodeEnvOp[Unit] = SimpleState.modify(_.copy(inUDF = true))
  val remInUDF: CodeEnvOp[Unit] = SimpleState.modify(_.copy(inUDF = false))
  // rename columns
  val renameCols: CodeEnvOp[Boolean] = SimpleState.read(_.renameCols)
  val setRenameCols: CodeEnvOp[Unit] = SimpleState.modify(_.copy(renameCols = true))

  // the only thing that needs to be carried between top-level constructs is the query id
  val cleanTopEnv: CodeEnvOp[Unit] =
    SimpleState.modify(e => CodeState().copy(queryId = e.queryId))

  // only thing that needs to be carried btwn local queries (and main) is query id and table id
  val cleanQueryEnv: CodeEnvOp[Unit] =
    SimpleState.modify(e => CodeState().copy(queryId = e.queryId, tableId = e.tableId))

  // initializes query state on the q side
  val initQueryState: String = " .aq.initQueryState[];"

  /**
   * Wrapper for a kdb functional statement op[t;w;b;p]
   * @param t table
   * @param w where clause
   * @param b group-by
   * @param p projection
   * @return
   */
  def kdbFunctional(
    op: String,
    t: Option[String],
    w: Option[String],
    b: Option[String],
    p: Option[String]
  ): String = {
    val tc = t.getOrElse("")
    val wc = w.getOrElse("()")
    val bc = b.getOrElse("0b")
    val pc = p.getOrElse("()")
    s"$op[$tc;$wc;$bc;$pc]"
  }

  /**
   * Wrapper for kdb select statement
   * @param t table name
   * @param w where clause
   * @param b group-by clause
   * @param p projection clause
   * @return
   */
  def kdbSelect(t: Option[String], w: Option[String], b: Option[String], p: Option[String])
  : String =
    kdbFunctional("?", t, w, b, p)

  /**
   * Wrapper for kdb update statement
   * @param t
   * @param w
   * @param b
   * @param p
   * @return
   */
  def kdbUpdate(t: Option[String], w: Option[String], b: Option[String], p: Option[String])
  : String =
    kdbFunctional("!", t, w, b, p)

  /**
   * Wrapper for a kdb exec statement ?[t;w;();p]
   * @param t table
   * @param w where clause
   * @param p projection
   * @return
   */
  def kdbExec(t: Option[String], w: Option[String], p: Option[String]): String =
    kdbSelect(t, w, Some("()"), p)

  /**
   * Wrapper for kdb delete c1,...., c2 from t (column-wise deletion)
   * @param t
   * @param c
   * @return
   */
  def kdbDeleteCols(t: String, c: String): String = kdbUpdate(Some(t), None, None, Some(c))

  /**
   * Wrapper for kdb delete from t where .... (row-wise deletion)
   * @param t
   * @param w
   * @return
   */
  def kdbDeleteWhere(t: String, w: String): String = kdbUpdate(Some(t), Some(w), None, Some("`$()"))

  /**
   * Wrapper for kdb symbol datatype
   * @param s
   * @return
   */
  def kdbSym(s: String): String = "`" + s

  /**
   * Wrapper for kdb boolean datatype
   * @param b
   * @return
   */
  def kdbBool(b: Boolean): String = if (b) "1b" else "0b"

  // An empty kdb list
  val kdbEmptyList: CodeGen = SimpleState.unit("()")

  /**
   * Generate kdb code for a list of elements, sep separated. Must be wrapped in ()
   * as needed.
   * @param es list of elements
   * @param sep separator
   * @param f code generator for element
   * @tparam A
   * @return
   */
  def genCodeList[A](es: List[A], sep: String)(f: A => CodeGen): CodeGen =
    SimpleState.sequence(es.map(f)).map(_.mkString(sep))

  /**
   * Generate kdb code for a dictionary of expressions
   * @param es list of tuples of expression and key
   * @param fk and function to apply to the key
   * @return
   */
  def genExprDict(es: Seq[(Expr, String)], fk: String => String): CodeGen = es match {
    case (WildCard, _) :: Nil => genTableExpr(WildCard)
    case (v, n) :: Nil => genExpr(v).map(c => s"(enlist[${fk(n)}]!enlist $c)")
    case _ if !es.exists(_._1 == WildCard) =>
      val (values, rawKeys) = es.unzip
      val keys = rawKeys.map(str => s"(${fk(str)})").mkString(";")
      genCodeList(values.toList, ";")(genExpr).map(vs => s"($keys)!($vs)")
    // if there is a wild card, we should generate each separately and then append
    case _ => genCodeList(es.toList.map(e => List(e)), ",")(genExprDict(_, fk))
  }

  /**
   * Generate code for an expression
   * @param e
   * @return
   */
  def genExpr(e: Expr): CodeGen = e match {
    case b: BinExpr => genBinExpr(b)
    case u: UnExpr => genUnExpr(u)
    case id: Id => genId(id)
    case lit: Lit => genLit(lit)
    case fc: FunCall => genFunCall(fc)
    case aix: ArrayIndex => genArrayIndex(aix)
    case ce: Case => genCaseExpr(ce)
    case te: TableExpr => genTableExpr(te)
  }

  /**
   * if singleton, add enlist call
   * @param code
   * @param len length of collection that generated code
   * @return
   */
  def makeList(code: String, len: Int): String = if (len == 1) s"enlist $code" else code

  /**
   * Convenience method to generate expression lists.
   * If singleton, and wrap=true, prepends `enlist` call.
   * @param es expressions
   * @param wrap if true adds () around the result
   * @return
   */
  def genExprList(es: Seq[Expr], wrap: Boolean): CodeGen = es match {
    case Nil => kdbEmptyList
    case x :: xs =>
      val ge = genCodeList(es.toList, ";")(genExpr)
      if(wrap) ge.map(l => makeList("(" + l + ")", es.length)) else ge
  }

  /**
   * Generate code for binary expression operator
   * @param op
   * @return
   */
  def genBinOp(op: BinOp): String = op match {
    case Plus => "+"
    case Minus => "-"
    case Times => "*"
    case Div => "%"
    case Exp => ".aq.pow"
    case Lt => "<"
    case Le => "<="
    case Gt => ">"
    case Ge => ">="
    case Eq => "="
    case Neq => "<>"
    case Land => "&"
    case Lor => "|"
  }

  /**
   * Generate code for binary expression
   * @param b
   * @return
   */
  def genBinExpr(b: BinExpr): CodeGen =
    for (
      l <- genExpr(b.l);
      r <- genExpr(b.r)
    ) yield  s"(${genBinOp(b.op)}; $l; $r)"

  /**
   * Generate code for unary expression operator
   * @param op
   * @return
   */
  def genUnOp(op: UnOp): String = op match {
    case Neg => ".aq.neg"
    case Not => ".aq.not"
  }

  /**
   * Generate code for unary expression
   * @param u
   * @return
   */
  def genUnExpr(u: UnExpr): CodeGen =
    for(e <- genExpr(u.v)) yield s"(${genUnOp(u.op)}}; $e)"

  /**
   * Generate code for an ID. Distinguishes between column lookup and rval in a function
   * @param id
   * @return
   */
  def genId(id: Id): CodeGen =
    for (
      nv <- nameAsVar;
      iu <- inUDF) yield
      if (iu)
        s".aq.funEnlist ${id.v}"
      else if (nv)
        id.v
      else getColLookup(id.v)

  /**
   * Generate code to lookup a column
   * @param str
   * @return
   */
  def getColLookup(str: String): String = s"{x^.aq.cd x} ${kdbSym(str)}"

  /**
   * Generate code for a literal
   * @param l
   * @return
   */
  def genLit(l: Lit): CodeGen = SimpleState.unit { l match {
    case IntLit(v) => v.toString
    case FloatLit(v) => v.toString
    // as symbol
    case StringLit(v) => s"""enlist `$$"$v""""
    case DateLit(v) => s""""D"$$"$v""""
    case TimestampLit(v) => s""""P"$$"$v""""
    case BooleanLit(v) => if (v) "1b" else "0b"
    }
  }

  /**
   * Generate code for a table expression. For wildcard we generate code that creates a
   * dictionary of columns to use in a projection.
   * @param t
   * @return
   */
  def genTableExpr(t: TableExpr): CodeGen = t match {
    case RowId => SimpleState.unit(kdbSym("i"))
    case ColumnAccess(tn, c) => SimpleState.unit(getColLookup(tn + "." + c))
    case WildCard => for (t <- getCurrTable) yield s"(.aq.wildCard $t)"
  }

  /**
   * Generate code for array indexing
   * @param expr
   * @return
   */
  def genArrayIndex(expr: ArrayIndex): CodeGen = {
    val op = expr.ix match {
      case Even => ".aq.indexEven"
      case Odd => ".aq.indexOdd"
      case Every(n) => s".aq.indexEveryN[$n; ]"
    }
    for(ec <- genExpr(expr.e)) yield s"($op; $ec)"
  }

  /**
   * Generate appropriate function name for call
   * @param s raw function name
   * @param nargs number of arguments
   * @return
   */
  def genFunName(s: String, nargs: Int): String = {
    val lcs = s.toLowerCase
    val woArgs = (lcs, None)
    val wArgs = (lcs, Some(nargs))
    if (BUILT_INS.contains(wArgs))
      ".aq." + lcs + nargs
    else if (BUILT_INS.contains(woArgs))
      ".aq." + lcs
    else
      s
  }

  /**
   * Add enlist to an unwrapped list, used for variadic function calls
   * @param code
   * @return
   */
  def addEnlist(code: String): String = s"(enlist;$code)"

  /**
   * Generate code for a function call
   * @param expr
   * @return
   */
  def genFunCall(expr: FunCall): CodeGen = {
    val origF = expr.f
    val f = genFunName(origF, expr.args.length)
    // replace * in COUNT(*) with ROWID, as done in kdb+( => count i)
    val cleanArgs =
      expr.args.map(e => if (origF.toUpperCase == "COUNT" && e == WildCard) RowId else e)
    // generate code for args
    val args = genExprList(cleanArgs, wrap = false)
    val variadic = VARIADIC.contains(origF.toLowerCase)
    for(
      as <- args;
      g <- isGrouped
    ) yield {
      val cleanF = if (g) s"$f'" else f
      val cleanAs = if (variadic) addEnlist(as) else as
      s"($cleanF;$cleanAs)"
    }
  }

  /**
   * Generate code for if-then pair, returns (enlist; c; t)
   * @param it
   * @return
   */
  def genIfThen(it: IfThen): CodeGen =
    for (
      c <- genExpr(it.c);
      t <- genExpr(it.t)
    ) yield addEnlist(s"$c; $t")

  /**
   * Generate code for a case expression
   * @param expr
   * @return
   */
  def genCaseExpr(expr: Case): CodeGen = {
    val ifThens = genCodeList(expr.when, ";")(genIfThen)
    val elseClause = expr.e.map(genExpr).getOrElse(kdbEmptyList)
    val cond = expr.cond.map(genExpr).getOrElse(kdbEmptyList)
    for (
      c <- cond;
      it <- ifThens;
      ec <- elseClause
    ) yield s"(.aq.cond; $c;${addEnlist(it)}; $ec)"
  }

  /**
   * Generate code for a relational algebra plan
   * @param r
   * @return
   */
  def genRelAlg(r: RelAlg): CodeGen = r match {
    case t: Table => genTable(t)
    case f: Filter => genFilter(f)
    case rf: ReorderFilter => genReorderFilter(rf)
    case s: SortBy => genSortBy(s)
    case cs: ColSortBy => genColSortBy(cs)
    case g: GroupBy => genGroupBy(g)
    case p: Project => genProject(p)
    case ba: BottomApply => genBottomApply(ba)
    case ta: TopApply => genTopApply(ta)
    case j: Join => genJoin(j)
  }

  /**
   * Generate a fresh temporary table name
   * @return
   */
  def genTempTableName(): CodeGen =
    for(id <- getTableId; _ <- incrTableId) yield "aq__t" + id

  /**
   * Generate initialization code for a table
   * @param t
   * @return
   */
  def genTable(t: Table): CodeGen = {
    val table = t.n
    val alias = t.alias.getOrElse(table)
    for (
      temp <- genTempTableName();
      _ <- setCurrTable(temp);
      rename <- renameCols
    ) yield s""" $temp:.aq.initTable[$table;"$alias";${kdbBool(rename)}];"""
  }

  /**
   * Generate code for applying a function at the bottom of a relational algebra plan
   * e.g. SELECT * FROM f(t,1,2)
   * @param b
   * @return
   */
  def genBottomApply(b: BottomApply): CodeGen = {
    for (
      temp <- genTempTableName();
      _ <- setCurrTable(temp);
    // we set as name as var, so that ids are not interpreted as column lookups
    // allows things like concatenate(t, t1) where t and t1 are local variables
      _ <- setNameAsVar;
      call <- genFunCall(FunCall(b.f, b.args));
    // we no longer need this restriction after generating the call code
      _ <- remNameAsVar
    ) yield s""" $temp:.aq.initTable[eval $call;"";${kdbBool(false)}];"""
  }

  /**
   * Generate code for a filter (i.e. selections)
   * @param f
   * @return
   */
  def genFilter(f: Filter): CodeGen = {
    for(
      before <- genRelAlg(f.t);
      t <- getCurrTable;
      fs <- genExprList(f.fs, wrap = true)
    ) yield s"$before\n $t:${kdbSelect(Some(t), Some(fs), None, None)};"
  }

  /**
   * Generate code for a filter in which selections can be freely reordered according to indices
   * @param f
   * @return
   */
  def genReorderFilter(f: ReorderFilter): CodeGen = {
    // no need for special code if just one filter
    if (f.fs.length == 1)
      genFilter(Filter(f.t, f.fs.toList, f.attr))
    else
      for (
        before <- genRelAlg(f.t);
        t <- getCurrTable;
        fs <- genExprList(f.fs, wrap = true)
      ) yield {
        val reordered = s".aq.reorderFilter[$t;$fs]"
        s"$before\n $t:${kdbSelect(Some(t), Some(reordered), None, None)};"
      }
  }

  /**
   * "Infer" a column name from an expression. Currently only the simplest possible.
   * This could potentially be changed but it is important to check if this causes issues
   * elsewhere, as there are some assumptions that depend on upsert semantics in kdb+.
   * @param e
   * @return
   */
  def inferColName(e: Expr): Option[String] = e match {
    case Id(v) => Some(v)
    case ColumnAccess(t, c) => Some("t" + "." + c)
    case _ => None
  }

  /**
   * Add column names to a sequence of expressions and possible column names
   * @param es
   * @return
   */
  def addColNames(es: Seq[(Expr, Option[String])]): Seq[(Expr, String)] = {
    es.foldLeft((0, List.empty[(Expr, String)])) { case ((id, acc), (e, n)) =>
      val (next, name) =
        n.map((id, _))
          .orElse(inferColName(e).map((id, _)))
          .getOrElse((id + 1, "c__" + id))
      (next, (e, name) :: acc)
    }._2.reverse
  }

  /**
   * Generate code for group by. Sets code env to grouped
   * @param g
   * @return
   */
  def genGroupBy(g: GroupBy): CodeGen = g match {
    case GroupBy(src, gs, Nil, _) =>
      val cleanGroups = addColNames(gs)
      for (
        before <- genRelAlg(src);
        t <- getCurrTable;
        groups <- genExprDict(cleanGroups, getColLookup);
        _ <- setGrouped;
        allCols <- genTableExpr(WildCard)
      ) yield {
        val groupNames = cleanGroups.map(g => getColLookup(g._2)).mkString(";")
        // drop column names we've already accounted for
        val cleanProjections = s"($groupNames) _ $allCols"
        s"$before\n $t:${kdbSelect(Some(t), None, Some(groups), Some(cleanProjections))};"
      }
    case GroupBy(src, gs, h @ x :: xs, _) => genRelAlg(Filter(GroupBy(src, gs, Nil), h))
  }

  /**
   * Generate code for projection
   * @param p
   * @return
   */
  def genProject(p: Project): CodeGen = {
    val cleanProjections = addColNames(p.ps)
    for (
      before <- genRelAlg(p.t);
      t <- getCurrTable;
      ps <- genExprDict(cleanProjections, kdbSym)
    ) yield s"$before\n $t:${kdbSelect(Some(t), None, None, Some(ps))};"
  }

  /**
   * Generate code for a top-apply function. Currently only allows: SHOW/DISTINCT/EXEC ARRAYS
   * @param tp
   * @return
   */
  def genTopApply(tp: TopApply): CodeGen = {
    val f = genFunName(tp.f, 1)
    for(
      before <- genRelAlg(tp.t);
      t <- getCurrTable
    ) yield s"$before\n $t:$f[$t];"
  }

  /**
   * Get q built-in for a given order direction.
   * @param o
   * @return
   */
  def getOrderDirection(o: OrderDirection): String = o match {
    case Asc => "iasc"
    case Desc => "idesc"
  }

  /**
   * Generate code for column-based sort-by
   * @param sb
   * @return
   */
  def genColSortBy(sb: ColSortBy): CodeGen = {
    for(
      before <- genRelAlg(sb.t);
      orders <- genOrderTuples(sb.os);
      cols <- genExprList(sb.cols.toList, wrap = false);
      grouped <- isGrouped;
      t <- getCurrTable
    ) yield {
      val call = if (grouped) ".aq.sortGrouped" else ".aq.sort"
      s"$before\n $t:$call[$t;$orders;($cols)];"
    }
  }

  def genOrderTuples(os: List[(OrderDirection, Expr)]): CodeGen = {
    // rewrite as function call, easiest way to take advantage of existing code generators
    val rewritten = os.map { case (d, c) => FunCall(getOrderDirection(d), List(c)) }
    // don't want each modifier on functions here, since we apply the sorting
    // on a per-group basis already (i.e. there is a top-level each in the sorting call)
    for(
      s <- SimpleState.get;
      _ <- remGrouped;
      order <- genExprList(rewritten, wrap = true);
      _ <- SimpleState.set(s)
    ) yield order
  }

  /**
   * Generate code for normal sort-by
   * @param sb
   * @return
   */
  def genSortBy(sb: SortBy): CodeGen =
    // just defer to col-based with all cols
    genRelAlg(ColSortBy(sb.t, sb.os, Set(WildCard)))

  /**
   * Generate code for a join
   * @param j
   * @return
   */
  def genJoin(j: Join): CodeGen =
    for(
      _ <- setRenameCols;
      left <- genRelAlg(j.l);
      lt <- getCurrTable;
      right <- genRelAlg(j.r);
      rt <- getCurrTable;
      cols <- genExprList(j.cond, wrap = true);
      jt <- genTempTableName();
      _ <- setCurrTable(jt)
    ) yield {
      val before = s"$left\n$right"
      val join = j.jt match {
        case Cross => s"$lt cross $rt"
        case InnerJoinUsing => s".aq.joinUsing[.aq.ej;$lt;$rt;$cols]"
        case FullOuterJoinUsing => s".aq.joinUsing[.aq.foju;$lt;$rt;$cols]"
      }
      s"$before\n $jt:$join;"
    }

  /**
   * Generate code to relabel columns in a table
   * @param cols columns to rename to
   * @param t table name
   * @return
   */
  def genRelabelCols(cols: List[String], t: String): String = cols match {
    case Nil => t
    case _ => cols.map(kdbSym).mkString("") + " xcol " + t
  }

  /**
   * Generate code for a local query
   * @param l local query
   * @return
   */
  def genLocalQuery(l: (String, List[String], RelAlg)): CodeGen = l match {
    case (n, cols, plan) =>
      genPlan(plan, (t: String) => s"$n:${genRelabelCols(cols, t)};")
  }

  /**
   * Generate code for a relational algebra plan (reinitializes query state before new code)
   * @param plan plan to generate code for
   * @param f function to apply to final result table (can be identity just to return, or can
   *          be used for further processing)
   * @return
   */
  def genPlan(plan: RelAlg, f: String => String): CodeGen =
    for(
      _ <- cleanQueryEnv;
      code <- genRelAlg(plan);
      t <- getCurrTable
    ) yield s"$initQueryState\n$code\n ${f(t)}"

  /**
   * Generate code for a complete query, with possible local queries
   * @param q
   * @return
   */
  def genQuery(q: Query): CodeGen = {
    val locals = SimpleState.sequence(q.local.map(genLocalQuery)).map(_.mkString("\n"))
    val main = genPlan(q.main, identity)
    for (
      ls <- locals;
      m <- main
    ) yield if (q.local.nonEmpty) s"$ls\n$m" else s"$m"
  }

  /**
   * Get query name
   * @param id
   * @return
   */
  def getQueryName(id: Int): String = ".aq.q" + id

  /**
   * Generate code for a local variable assignment in UDF body
   * @param a
   * @return
   */
  def genAssign(a: Assign): CodeGen =
    for (rval <- genExpr(a.e)) yield s" ${a.n}:eval $rval"

  /**
   * Generate code for an UDF
   * @param u
   * @return
   */
  def genUDF(u: UDF): CodeGen = {
    for (
      _ <- setNameAsVar;
      _ <- setInUDF;
      body <- genCodeList(u.cs, ";\n ")({
        case Left(a) => genAssign(a)
        case Right(e) => genExpr(e).map("eval " + _)
      });
      _ <- remNameAsVar;
      _ <- remInUDF
    ) yield
      s"${u.n}:{[${u.args.mkString(";")}]\n $body\n };"
  }

  /**
   * Generate code to create a table based on a query or schema declaration
   * @param c
   * @return
   */
  def genCreate(c: Create): CodeGen = c match {
    case Create(n, Right(q)) =>
      for(query <- genQuery(q)) yield s".aq.show ${kdbSym(n)} set {\n$query\n }[];"
    case Create(n, Left(s)) =>
      SimpleState.unit(s".aq.show ${kdbSym(n)} set ${genSchema(s)};")
  }

  /**
   * Generate code for a schema
   * @param ls
   * @return
   */
  def genSchema(ls: List[(String, TypeName)]): String =
    "([]"+ ls.map{ case (c, t) => s"""$c:"${getTypeCode(t)}"$$()"""}.mkString(";") +")"

  /**
   * Get the appropriate kdb type code. Note that ints get mapped to longs, which are default
   * in newer kdb+ (using version KDB+ 3.3 2015.11.03 for this)
   * @param t
   * @return
   */
  def getTypeCode(t: TypeName): String = t match {
    case TypeInt => "j"
    case TypeFloat => "f"
    case TypeDate => "d"
    case TypeTimestamp => "p"
    case TypeString => "s"
    case TypeBoolean => "b"
  }

  /**
   * Generate code for a load/save operation
   * @param io
   * @return
   */
  def genDataIO(io: DataIO): CodeGen = io match {
    case Load(file, table, sep) =>
      SimpleState.unit(s""".aq.show .aq.load[${getFileHandle(file)};"$sep";${kdbSym(table)}];""")
    case Save(file, q, sep) =>
      for (query <- genQuery(q)) yield
        s""".aq.show .aq.save[${getFileHandle(file)};"$sep"; ] {\n $query\n }[];"""
  }

  /**
   * Get the kdb file-handle
   * @param f
   * @return
   */
  def getFileHandle(f: String): String = s"""hsym `$$"$f""""

  /**
   * Generate code for an insertion into an existing table. Can insert values or from query
   * @param i
   * @return
   */
  def genInsert(i: Insert): CodeGen = {
    val sorted: CodeGen =
      if (i.order.nonEmpty)
        genQuery(Query(Nil, SortBy(Table(i.n), i.order))).map(c => s"\n {\n $c \n }[]")
      else
        SimpleState.unit(i.n)
    val data: CodeGen = i.src match {
      case Right(q) => genQuery(q).map(c => s"\n {\n $c\n }[]")
      case Left(es) => genExprList(es, wrap = false).map(c => s"eval ${addEnlist(c)}")
    }
    val modifier: CodeGen =
      if (i.modifier.nonEmpty)
        SimpleState.unit(i.modifier.map(kdbSym).mkString(""))
      else
        kdbEmptyList
    for (
      s <- sorted;
      d <- data;
      m <- modifier
    ) yield s".aq.insert[${kdbSym(i.n)};$s;$m;$d]"
  }

  /**
   * Generate code for an update or delete opetarion
   * @param m
   * @return
   */
  def genModificationQuery(m: ModificationQuery): CodeGen = m match {
    case d: Delete => genDelete(d)
    case u: Update => genUpdate(u)
  }

  /**
   * Generate code for an update operation
   * @param u
   * @return
   */
  def genUpdate(u: Update): CodeGen = {
    val wrappedT = if (u.order.nonEmpty) SortBy(Table(u.t), u.order) else Table(u.t)
    val ignoreResult = (_: String) => ""
    val cleanGroups: Seq[(Expr, String)] = addColNames(u.groupby.map((_, None)))
    val vectorName = "aq__ix"
    u match {
      case Update(t, sets, _, where, groupby, having) =>
        for (
          init <- genPlan(wrappedT, ignoreResult);
          sortedT <- getCurrTable;
          w <- genExprList(where, wrap = true);
          g <- genExprDict(cleanGroups, getColLookup);
          s <- genExprDict(sets.map(_.swap), getColLookup);
          ix <- genBooleanVector(sortedT, where, groupby, having, vectorName)
        ) yield {
          val gClean = if (cleanGroups.nonEmpty) Some(g) else None
          having match {
            // if there is no having clause, we can handle in a single kdb update op
            case Nil =>
              s".aq.show ${kdbSym(t)} set {\n$init\n ${kdbUpdate(Some(sortedT), Some(w), gClean, Some(s))}\n }[];"
            // if there is a having, we need to use a helper to solve for indices that need
            // to be updated, and pass this to an update op
            case _ =>
              s".aq.show ${kdbSym(t)} set {\n$init\n$ix\n\n ${
                kdbUpdate(Some(sortedT), Some("enlist " + vectorName), gClean, Some(s))
              }\n }[];"
          }
        }
    }
  }

  /**
   * Generate code for a delete operation
   * @param d
   * @return
   */
  def genDelete(d: Delete): CodeGen = {
    val wrappedT= if (d.order.nonEmpty) SortBy(Table(d.t), d.order) else Table(d.t)
    val vectorName = "aq__ix"
    val ignoreResult = (_: String) => ""
    d match {
      case Delete(t, Left(cs), _, _, _) =>
        for (
          init <- genPlan(wrappedT, ignoreResult);
          cols <- genExprList(cs, wrap = true)
        ) yield s".aq.show ${kdbSym(t)} set {\n$init\n ${kdbDeleteCols(t, cols)}\n }[];"
      case Delete(t, Right(fs), _, groupby, having) =>
        for (
          init <- genPlan(wrappedT, ignoreResult);
          sortedT <- getCurrTable;
          w <- genExprList(fs, wrap = true);
          ix <- genBooleanVector(sortedT, fs, groupby, having, vectorName)
        ) yield {
          having match {
            // if there is no having clause, we can handle in a single kdb delete op
            case Nil => s".aq.show ${kdbSym(t)} set {\n$init\n ${kdbDeleteWhere(sortedT, w)}\n }[];"
            // if there is a having, we need to use a helper to solve for indices that need
            // to be deleted, and pass this to a delete op
            case _ => s".aq.show ${kdbSym(t)} set {\n$init\n$ix\n\n ${
              kdbDeleteWhere(sortedT, "enlist " + vectorName)
            }\n }[];"
          }
        }
    }
  }

  /**
   * Generates a boolean vector that can be used to update/delete. Boolean vector is of same
   * length as sortedT.
   * @param sortedT possibly sorted table that will be modified
   * @param where where clause
   * @param group group clause
   * @param having having clause
   * @param vec the name to store the boolean vector in
   * @return
   */
  def genBooleanVector(
    sortedT: String,
    where: List[Expr],
    group: List[Expr],
    having: List[Expr],
    vec: String): CodeGen = {
      // remove all group names, we want all cols to be grouped so that having expressions
      // eval as expected
      val cleanGroups: List[(Expr, Option[String])] = group.map((_, None))
      // collect all columns, add vecResult column (which just masks virtual index col)
      val project: List[(Expr, Option[String])] = List((WildCard, None), (RowId, Some(vec)))
      val query = GroupBy(Filter(Project(Table(sortedT), project), where), cleanGroups, having)
      // based on indices that return from query, assign true to those locations, false to others
      // this vector can then be used to perform update/delete ops by applying `where` to it
      val assignToIndex =
        (res: String) => s" $vec:@[(count $sortedT)#0b;(ungroup 0!$res)${kdbSym(vec)};:;1b]"
      val cleanState = " .aq.initQueryState[];"
      // remove annotation for grouped, defer that responsibility to main query
      // but use genRelAlg, as doesn't reset environment, since we don't want to clobber potentially
      // sorted table t
      for (
        indexQuery <- genRelAlg(query);
        indexTable <- getCurrTable;
        _ <- remGrouped
      ) yield s"$cleanState\n$indexQuery\n ${assignToIndex(indexTable)};"
  }

  /**
   * Generate code for a top-level construct
   * @param t
   * @return
   */
  def genTopLevel(t: TopLevel): CodeGen = t match {
    case q: Query =>
      for (
        id <- getQueryId;
        _ <- incrQueryId;
        code <- genQuery(q)
      ) yield {
        val qName = getQueryName(id)
        // generate the query code and run
        s"$qName:{\n$code\n };\n$qName[]"
      }
    case VerbatimCode(c) => SimpleState.unit("// verbatim code\n" + c)
    case io: DataIO => genDataIO(io)
    case u: UDF => genUDF(u)
    case c: Create => genCreate(c)
    case i: Insert => genInsert(i)
    case m: ModificationQuery => genModificationQuery(m)
  }

  /**
   * Generate the basic aquery functions from .aq namespace to add to code generated (to make
   * q file movable)
   * @return
   */
  def getPrelude: CodeGen = SimpleState.unit {
    val contents = Source.fromURL(getClass.getClassLoader.getResource("q/base.q"))
    contents.getLines().toList.mkString("\n")
  }

  /**
   * Generate executable q code from an aquery AST
   * @param prog
   * @return
   */
  def generate(prog: Seq[TopLevel]): String = {
    val translator = for (
      prelude <- getPrelude;
      msg <- SimpleState.unit("// Translation begins here");
      code <- genCodeList(prog.toList, "\n\n")(e => for (_ <- cleanTopEnv; c <- genTopLevel(e)) yield c)
    ) yield s"$prelude\n$msg\n$code\n"
    // generate code with initially clean state
    translator(CodeState())._2
  }
 }

object KdbGenerator {
  def apply(): KdbGenerator = new KdbGenerator()
  def generate(prog: Seq[TopLevel]): String = new KdbGenerator().generate(prog)
}