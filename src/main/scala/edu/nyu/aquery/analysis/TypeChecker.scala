package edu.nyu.aquery.analysis

import edu.nyu.aquery.ast._
import AnalysisTypes._


/**
 * Performs a "soft" type check on an aquery program. We call this soft as a lot of information
 * won't be available until runtime, given kdb+'s dynamic typing. So the main goal of this check
 * is to catch errors already known at translation time.
 *
 * Note that for the most part, we deal with unknown types, so this makes it hard to justify
 * a very in-depth analysis of UDFs (since most will return unknown type), and similar constructs.
 * However, this does catch some common issues and can be easily extended to cover more cases.
 *
 * @param info contains information related to functions, particularly the number of arguments
 *             required in a call (for UDFs), along with return types and argument types (for built
 *             ins)
 */
class TypeChecker(info: FunctionInfo) {
  // convenience wrapper around type error
  private def err(t: TypeTag, f: TypeTag, e: Expr) = TypeError(t, f, e.pos)

  /**
   * Check that the expression has type `expect`. Returns this error and any errors that
   * were found during the type checking of the expression itself
   * @param expect expected type
   * @param expr
   * @return
   */
  def checkTypeTag(expect: Set[TypeTag], expr: Expr): Seq[AnalysisError] = {
    val (t, errors) = checkExpr(expr)
    val tagError =
      if (expect.contains(t) || t == TUnknown || expect.contains(TUnknown))
        List()
      else
        // just provide first option for error message
        List(err(expect.head, t, expr))
    errors ++ tagError
  }

  /**
   * Type check binary expressions
   * @param expr
   * @return
   */
  def checkBinExpr(expr: BinExpr): (TypeTag, Seq[AnalysisError]) =  expr.op match {
    // && and || are assumed to be solely for booleans, users can use min/max for numeric
    case Land | Lor =>
      (TBoolean, checkTypeTag(bool, expr.l) ++ checkTypeTag(bool, expr.r))
    case Lt | Le | Gt | Ge  =>
      (TBoolean, checkTypeTag(num, expr.l) ++ checkTypeTag(num, expr.r))
    // requires that the types of the args agree
    case Eq | Neq =>
      val (leftType, leftErrors) = checkExpr(expr.l)
      (TBoolean, leftErrors ++ checkTypeTag(Set(leftType), expr.r))
    // can use both numeric and boolean to allow helpful boolean arithmetic e.g. c1 * c2 > 2
    case Plus | Minus | Times | Div | Exp =>
      (TNumeric, checkTypeTag(numAndBool,  expr.l) ++ checkTypeTag(numAndBool, expr.r))
  }

  /**
   * Type check unary expressions
   * @param expr
   * @return
   */
  def checkUnaryExpr(expr: UnExpr): (TypeTag, Seq[AnalysisError]) = expr.op match {
    case Not =>
      (TBoolean, checkTypeTag(bool, expr.v))
    case Neg =>
      (TNumeric, checkTypeTag(num, expr.v))
  }

  /**
   * Type check function calls. For built-ins this includes checking the type of the actual
   * parameters versus those of the formals (defined in edu.nyu.aquery.analysis.FunctionInfo) and
   * returning the predefined type. For UDFs this only includes checking the number of arguments
   * and will always return TUnknown.
   *
   * For array indexing (which is in essence a function call), we always return TUnknown, and
   * simply check the array expression for any errors. A remaining todo is to check the size of
   * the array expression before indexing
   * @param expr
   * @return
   */
  def checkCallExpr(expr: CallExpr): (TypeTag, Seq[AnalysisError]) = expr match {
    case FunCall(f, args) =>
      // if we have a match, then check arguments etc and get appropriate return type
      info(f).map { x =>
        val typesAndErrors = args.map(e => checkExpr(e))
        val argTypes = typesAndErrors.map(_._1)
        val argErrors = typesAndErrors.flatMap(_._2)
        val ret = x.signature.lift(argTypes)
        val badCall = ret.map(_ => Nil).getOrElse(BadCall(f, expr.pos) :: Nil)
        (ret.getOrElse(TUnknown), badCall ++ argErrors)
      }.getOrElse {
        (TUnknown, args.flatMap(a => checkExpr(a)._2))
      }
    // TODO: size checks as part of type analysis
    case ArrayIndex(e, _) => (TUnknown, checkExpr(e)._2)
  }

  /**
   * A case expression is well typed if:
   *  - If there is a initial expression, then when conditions match that type (or are unknown),
   *    otherwise all when conditions are boolean (or unknown)
   *  - The types along all return branches must agree
   * @param c
   * @return
   */
  def checkCaseExpr(c: Case): (TypeTag, Seq[AnalysisError]) = c match {
    case Case(cond, when, e) =>
      // if no case expression, then conditions in if-else must be boolean
      val (condType, condErrors) = cond.map(checkExpr).getOrElse((TBoolean, Nil))
      // all branches should be boolean conditions or the same type as the case
      val ifErrors = when.flatMap(w => checkTypeTag(Set(condType), w.c))
      // first condition defines type of branches
      val (elseType, whenErrors) = when match {
        // go ahead with unknown type tag, and report missing as error
        case Nil => (TUnknown, List(err(TBoolean, TUnit, c)))
        case x :: Nil => checkExpr(x.t)
        case x :: xs =>
          val (thenType, errs) = checkExpr(x.t)
          val moreErrs = xs.flatMap(checkTypeTag(Set(thenType), _))
          (thenType, errs ++ moreErrs)
      }
      val elseErrors = e.map(checkTypeTag(Set(elseType), _)).getOrElse(Nil)

      (elseType, condErrors ++ ifErrors ++ whenErrors ++ elseErrors)
  }

  /**
   * Provides types to literals. We treat date/timestamp as numeric
   * @param e
   * @return
   */
  def checkLit(e: Lit): (TypeTag, Seq[AnalysisError]) = {
    val tag = e match {
      case IntLit(_) => TNumeric
      case FloatLit(_) => TNumeric
      case StringLit(_) => TString
      case DateLit(_) =>  TNumeric
      case BooleanLit(_) => TBoolean
      case TimestampLit(_) => TNumeric
    }
    (tag, Nil)
  }

  /**
   * Type check an expression
   * @param expr
   * @return
   */
  def checkExpr(expr: Expr): (TypeTag, Seq[AnalysisError]) = expr match {
    case bin: BinExpr => checkBinExpr(bin)
    case un: UnExpr => checkUnaryExpr(un)
    case call: CallExpr => checkCallExpr(call)
    case caseE: Case => checkCaseExpr(caseE)
    case lit: Lit => checkLit(lit)
    case Id(_) => (TUnknown, Nil)
    case RowId => (TNumeric, Nil)
    case ColumnAccess(_, _) | WildCard => (TUnknown, Nil)
  }

  /**
   * Recursively type check a relational algebra operator and its arguments.
   * Filter and having must be boolean expressions, similarly for conditions ina join
   * @param r
   * @return
   */
  def checkRelAlg(r: RelAlg): Seq[AnalysisError] = {
    val selfErrors = r match {
      case Filter(_, fs, _) => r.expr.flatMap(checkTypeTag(bool, _))
      case GroupBy(_, _, having, _) =>
        // note that we check having twice, just for simplicity of code
        having.flatMap(checkTypeTag(bool, _)) ++ r.expr.flatMap(checkExpr(_)._2)
      case _: Join => r.expr.flatMap(checkTypeTag(bool, _))
      case _ => r.expr.flatMap(checkExpr(_)._2)
    }
    // recursively check
    selfErrors ++ r.children.flatMap(checkRelAlg)
  }

  /**
   * Check local queries and the main query in a full query construct
   * @param q
   * @return
   */
  def checkQuery(q: Query): Seq[AnalysisError] =
    q.local.flatMap(lq => checkRelAlg(lq._3)) ++ checkRelAlg(q.main)

  /**
   * Check update and delete. Imposes similar restrictions on where and having clauses as
   * checkRelAlg
   * @param q
   * @return
   */
  def checkModificationQuery(q: ModificationQuery): Seq[AnalysisError] = q match {
    case Update(_, u, _, w, g, h) =>
      u.flatMap(c => checkExpr(c._2)._2) ++
        w.flatMap(c => checkTypeTag(bool, c)) ++
        g.flatMap(c => checkExpr(c)._2) ++
        h.flatMap(c => checkTypeTag(bool, c))
    case Delete(_, del, _, g, h) =>
      (del match {
        case Right(w) => w.flatMap(c => checkTypeTag(bool, c))
        case _ => Nil
      }) ++
        g.flatMap(c => checkExpr(c)._2) ++
        h.flatMap(c => checkTypeTag(bool, c))
  }

  /**
   * Checks table creation and insertion. Note that this DOES NOT check that the values
   * being inserted into a table match the appropriate types. This stems from the observation
   * that for the most part we won't have the types of columns until runtime, so most will be
   * TUnknown, making this check unlikely to spot meaningful issues.
   * @param m
   * @return
   */
  def checkTableModification(m: TableModification): Seq[AnalysisError] = m match {
    case Create(_, Right(q)) => checkQuery(q)
    case Insert(_, _, _, Left(e)) => e.flatMap(checkExpr(_)._2)
    case Insert(_, _, _, Right(q)) => checkQuery(q)
    case _ => Nil
  }

  /**
   * Checks UDF. Checks all expressions in the UDF body
   * @param f
   * @return
   */
  def checkUDF(f: UDF): Seq[AnalysisError] =
    f.cs.flatMap {
      case Left(as) => checkExpr(as.e)._2
      case Right(e) => checkExpr(e)._2
    }

  /**
   * Check a top level construct
   * @param com
   * @return
   */
  def checkTopLevel(com: TopLevel): Seq[AnalysisError] = com match {
    case q: Query => checkQuery(q)
    case mq: ModificationQuery => checkModificationQuery(mq)
    case tm: TableModification => checkTableModification(tm)
    case u: UDF => checkUDF(u)
    // we obviously don't type check verbatim code
    case v: VerbatimCode => List()
  }

  def typeCheck(prog: Seq[TopLevel]): Seq[AnalysisError] = prog.flatMap(checkTopLevel)

}

object TypeChecker {
  def apply(prog: Seq[TopLevel]): Seq[AnalysisError] = {
    // UDFs are checked solely for number of args to call
    val ctFunArgs = (s: FunctionInfo, f: UDF) => {
      s.write(f.n, new UDFSummary(f.n, { case x if x.length == f.args.length => TUnknown }))
    }
    // environment is collected sequentially
    val env = FunctionInfo(prog.collect { case f: UDF => f }, ctFunArgs)
    // type check with the current environment
    val checker = new TypeChecker(env)
    checker.typeCheck(prog)
  }
}
