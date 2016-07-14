import edu.nyu.aquery.analysis.JoinAnalysis._
import edu.nyu.aquery.ast._
import edu.nyu.aquery.parse.AqueryParser._

import org.scalatest.FunSuite

import scala.util.Try


class JoinAnalysisTestSuite extends FunSuite {
  test("query/filter type") {
    // determine if a query has a join
    val noJoinQuery = parse(query, "SELECT * FROM t WHERE c1 > 2").get
    assert(!hasJoin(noJoinQuery), "simple query")
    val joinQuery1 = parse(query, "SELECT * FROM t1, t2").get
    assert(hasJoin(joinQuery1), "cross")
    val joinQuery2 = parse(query, "SELECT * FROM t1 INNER JOIN t2 USING c1").get
    assert(hasJoin(joinQuery2), "inner join")

    // determine if a selection expression is "equality based"
    val exprs = parse(repsep(expr, ";"),
      """
        c1 = 2;
        not(f(c1)) ;
        not(not(c1 = c2 * 3)) ;
        f(c4 = 5) ;
        f(c1) != not(g(c2))
      """).get
    val results = exprs.map(isEqualitySelection)
    assert(results === List(true, false, true, false, true))
  }

  test("tables used/available") {
    // tables available in from clause
    val fromStr = "t1 as t_one, t2 INNER JOIN t3 USING cx"
    val from1 = parse(from, fromStr).get
    assert(tableNames(tablesAvailable(from1)) === Set("t1", "t_one", "t2", "t3"))
    assert(tableNames(tablesAvailable(from1.children.head)) === Set("t1", "t_one", "t2"))
    assert(tableNames(tablesAvailable(from1.children(1))) === Set("t3"))

    val query1 = parse(query, "SELECT c1, c2 FROM " + fromStr).get
    assert(tableNames(tablesAvailable(query1)) === Set("t1", "t_one", "t2", "t3"))

    // TODO: tables used
    // def tableNamesUsed(expr: Expr): Option[Set[String]] = expr match {
    val exprs = parse(repsep(expr, ";"),
      """
        f(c1) * t.c1 ;
        t.c1 * t.c2 / h(ot.c3) ;
        f(1, t.c1 / h(ot.c5 + t2.h))
      """).get
    val results = exprs.map(tableNamesUsed)
    val expect = List(None, Some(Set("t", "ot")), Some(Set("t", "ot", "t2")))
    assert(results === expect)
  }

  test("annotate") {
    // projection
    val p = Project(_: RelAlg, Nil)
    // filter on top of join
    val fj = Filter(_: RelAlg, Nil)
    // filter on lhs
    val fl = Filter(_: RelAlg, Nil)
    // lhs in join
    val l = Table("t1", Some("t_one"))
    // rhs in join
    val r = Table("t2")
    // join node
    val j = (l: RelAlg, r: RelAlg) => Join(Cross, l, r, Nil)
    // combined query
    val q: RelAlg = p(fj(j(fl(l), r)))
    val annotated = annotateWithAvailTableNames(q)
    val s1 = Set("t1", "t_one")
    val s2 = Set("t2")
    val all = s1 ++ s2

    val a = (s: Set[String]) => (n: RelAlg) => setAvailTableNames(n, s)
    val ap = p andThen a(all)
    val afj = fj andThen a(all)
    val afl = fl andThen a(s1)
    val al = a(s1)(l)
    val ar = a(s2)(r)
    val aj = (l: RelAlg, r: RelAlg) => a(all)(j(l, r))
    val expect = ap(afj(aj(afl(al), ar)))

    assert(annotated === expect)
  }

  test("place filter") {
    // ((t1 x t2) x t3) x t4
    val from1 = parse(from, "t1 as t_one, t2 INNER JOIN t3 USING cx, t4").get
    val exprs = parse(repsep(predicate | expr, ";"),
      """
        t1.c1 = t2.c1 ; // t1, t2
        t2.cx IS NOT NULL ; // t2
        t1.c3 + t3.c1 = 0 ; // t1, t3
        c1 = t.c2 ; // unknown (c1)
        t2.c3 > 100; // t2
        t_one.c = 100 // t1
      """
    ).get


    // note fold left (order matters)
    // higher in list of exprs above, higher in tree
    val result = exprs.foldLeft(from1)((acc, f) => placeFilter(acc, f))

    val filters = exprs.map(f => Filter(_: RelAlg, f :: Nil))
    val expectRaw =
      filters(3)(Join(Cross,
        filters(2)(
          Join(InnerJoinUsing,
            filters(0)(
              Join(Cross,
                filters(5)(Table("t1", Some("t_one"))),
                (filters(4) andThen filters(1))(Table("t2")),
              Nil)),
          Table("t3"),
          List(Id("cx")))),
        Table("t4"),
        Nil))

    val expect = annotateWithAvailTableNames(expectRaw)

    assert(result === expect)

    val shouldFail = parse(query, "SELECT c1, t.c2, c3 from t ASSUMING ASC c2 WHERE t.c1 > 100").get
    val result2 = Try(placeFilter(shouldFail, exprs.head))
    assert(result2.isFailure, "cannot use placeFilter in a non-join query")
  }
}
