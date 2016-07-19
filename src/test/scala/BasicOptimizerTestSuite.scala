import edu.nyu.aquery.analysis.JoinAnalysis
import edu.nyu.aquery.ast._
import edu.nyu.aquery.optimization.BasicOptimizer
import edu.nyu.aquery.optimization.BasicOptimizer.{orderEnv, ColSortBy, ReorderFilter}
import edu.nyu.aquery.parse.AqueryParser._

import org.scalatest.FunSuite


class BasicOptimizerTestSuite extends FunSuite {
  val optimizer = BasicOptimizer()

  def t(s: String) = parse(from, s).get
  def filter(t: RelAlg, s: String) = Filter(t, parse(repsep(predicate | expr, "AND"), s).get)
  def rfilter(t: RelAlg, s: String) = ReorderFilter(t, parse(repsep(predicate | expr, "AND"), s).get)
  def j(op: JoinType, t1: RelAlg, t2: RelAlg, s: String) = Join(op, t1, t2, parse(repsep(expr, ","), s).get)
  def join(t1: RelAlg, t2: RelAlg, s: String) = j(InnerJoinUsing, t1, t2, s)
  def cross(t1: RelAlg, t2: RelAlg) = j(Cross, t1, t2, "")
  def g(t1: RelAlg, s: String) = GroupBy(t1, parse(repsep(group, ","), s).get, Nil)
  def p(t1: RelAlg, s: String) = Project(t1, parse(repsep(projection, ","), s).get)

  def scols(t1: RelAlg, o: String, cols: String) =
    ColSortBy(t1, parse(order, o).get, parse(repsep(expr, ","), cols).get.toSet)

  def s(t: RelAlg, o: String) = SortBy(t, parse(order, o).get)

  def clearAttr(q: RelAlg): RelAlg = q.transform { case x => x.clearAttr }
  def clearAttr(m: Query): Query = {
    Query(local = m.local.map(l => (l._1, l._2, clearAttr(l._3))), main = clearAttr(m.main))
  }
  def clearAttr(s: Seq[TopLevel]): Seq[TopLevel] = s.map { case q: Query => clearAttr(q); case x => x}

  test("simplify sort") {
    val query1 = parse(query, "SELECT * FROM t ASSUMING ASC c1").get
    val env1 = Map("t" -> List((Asc, Id("c1"))))
    val result1 = optimizer.simplifySort(env1, query1)
    val expect1 = parse(query, "SELECT * FROM t").get
    assert(result1 === expect1, "can remove given existing order")

    val query2 = parse(query, "SELECT * FROM t1 INNER JOIN t2 USING c1 ASSUMING ASC c1").get
    val env2 = Map("t1" -> List((Asc, Id("c1"))))
    val result2 = optimizer.simplifySort(env2, query2)
    val expect2 = query2
    assert(result2 === expect2, "cannot remove in join")

    val query3 = query1
    val env3: orderEnv = Map()
    val result3 = optimizer.simplifySort(env3, query3)
    val expect3 = query3
    assert(result3 === expect3, "cannot remove if unknown")

    val query4 = parse(query, "SELECT * FROM t ASSUMING ASC t.c1, DESC c2").get
    val env4 = Map("t" -> List((Asc, Id("c1"))))
    val result4 = optimizer.simplifySort(env4, query4)
    val expect4 = parse(query, "SELECT * FROM t ASSUMING DESC c2").get
    assert(result4 === expect4, "can simplify if prefix")

    val query5 = query4
    val env5 = Map("t" -> List((Asc, Id("c3"))))
    val result5 = optimizer.simplifySort(env5, query5)
    val expect5 = query5
    assert(result5 === expect5, "if not prefix, cannot simplify")

    assert(
      List(result1, result2, result3, result4)
        .zip(List(env1, env2, env3, env4))
        .forall { case (q, e) => optimizer.simplifySort(e, q) === q},
      "idempotency"
    )
  }

  test("filterBeforeSort") {
    // selections before and after
    val query1 = parse(query,
      "SELECT * FROM t ASSUMING ASC c1 WHERE c2 = 10 AND c5 > 2 AND f(c0) > 2 AND c7 = 10"
    ).get
    // remove project by taking head of children
    val result1 = optimizer.filterBeforeSort(query1)
    val expect1 =
    Filter(
        SortBy(
          Filter(Table("t"), parse(repsep(expr, "AND"), " c2 = 10 AND c5 > 2").get),
          parse(order, "ASC c1").get
        ),
      parse(repsep(expr, "AND"), " f(c0) > 2 AND c7 = 10").get
    )
    assert(result1.children.head === expect1, "selections before and after sort")

    // selections only after sorting
    val query2 = parse(query,
      "SELECT * FROM t ASSUMING ASC t.c1 WHERE sums(c1) = 10 AND c2 = 10"
    ).get
    val result2 = optimizer.filterBeforeSort(query2)
    val expect2 = query2
    assert(result2 === expect2, "selections after sort (due to sums)")

    val query3 = parse(query,
      "SELECT * FROM t ASSUMING ASC t.c1 WHERE sum(c2) = 10 AND c5 = 10 AND c1 < 10"
    ).get
    val result3 = optimizer.filterBeforeSort(query3)
    val expect3 = query3
    assert(result3 === query3, "selections after sort (due to sum)")

    // selections only before sorting
    val query4 = parse(query,
    "SELECT * FROM t ASSUMING ASC t.c1, DESC c2 WHERE c1 = 10 AND c2 < 5 AND c6 IS NOT NULL"
    ).get
    val result4 = optimizer.filterBeforeSort(query4)
    val expect4 =
      SortBy(
        Filter(
          Table("t"),
          parse(repsep(predicate | expr, "AND"), "c1 = 10 AND c2 < 5 AND c6 IS NOT NULL").get
        ),
        parse(order, "ASC t.c1, DESC c2").get
      )
    assert(result4.children.head === expect4, "all selections before sort")


    assert(
      List(result1, result2, result3, result4).forall { o => optimizer.filterBeforeSort(o) === o},
      "idempotency"
    )
  }

  test("embedSort") {
    val query1 = parse(query, "SELECT * FROM t ASSUMING ASC c1, ASC c2 GROUP BY c3 * 2").get
    val result1 = optimizer.embedSort(query1)
    val expect1 = SortBy(
      GroupBy(Table("t"), (parse(expr, "c3 * 2").get, None) :: Nil, Nil),
      parse(order, "ASC c1, ASC c2").get
    )
    assert(result1.children.head === expect1, "no od in group-by")

    val query2 = parse(query, "SELECT * FROM t ASSUMING ASC c1, ASC c2 GROUP BY c1, c4 > 3 HAVING sums(c2) > 2").get
    val result2 = optimizer.embedSort(query2)
    val expect2 = query2
    assert(result2 === expect2, "od in having")

    val query3 = parse(query, "SELECT * FROM t ASSUMING ASC c1, ASC c2 GROUP BY t.c1").get
    val result3 = optimizer.embedSort(query3)
    val expect3 = SortBy(
      GroupBy(Table("t"), (parse(expr, "t.c1").get, None) :: Nil, Nil),
      parse(order, "ASC c1, ASC c2").get
    )
    assert(result3.children.head === expect3, "no od in group by")

    assert(
      List(result1, result2, result3).forall { o => optimizer.embedSort(o) === o},
      "idempotency"
    )

  }

  test("simplifyEmbeddedSort") {
    val optim = optimizer.embedSort _ andThen optimizer.simplifyEmbeddedSort
    val query1 = parse(query, "SELECT * FROM t ASSUMING ASC c1, ASC c2 GROUP BY c3 * 2").get
    val result1 = optim(query1)
    val expect1 = optimizer.embedSort(query1)
    assert(result1 === expect1, "not prefix")

    val query2 = parse(query, "SELECT * FROM t ASSUMING ASC c1, ASC c2 GROUP BY c1, c2").get
    val result2 = optim(query2)
    val expect2 = parse(query, "SELECT * FROM t GROUP BY c1, c2").get
    assert(result2 === expect2, "can remove sort completely, complete prefix")

    val query3 = parse(query, "SELECT * FROM t ASSUMING ASC c1, ASC c2 GROUP BY t.c1").get
    val result3 = optim(query3).children.head
    val expect3 = SortBy(
      GroupBy(Table("t"), (parse(expr, "t.c1").get, None) :: Nil, Nil),
      parse(order, "ASC c2").get
    )
    assert(result3 === expect3, "can remove c1 as t.c1 is group by")

    val query4 = parse(query, "SELECT * FROM t1, t2 ASSUMING ASC c1 GROUP BY t1.c1").get
    val result4 = optim(query4)
    val expect4 = optimizer.embedSort(query4)
    assert(result4 === expect4, "not prefix in cross if not exact same match")

    val query5 = parse(query, "SELECT * FROM t1, t2 ASSUMING ASC t1.c1 GROUP BY t1.c1").get
    val result5 = optim(query5)
    val expect5 = parse(query, "SELECT * FROM t1, t2 GROUP BY t1.c1").get
    assert(result5 === expect5, "can remove completely, exact match")

    assert(List(result1, result2, result3, result4, result5).forall { o =>
      optim(o) === o
    }, "idempotency")
  }

  test("pushFiltersJoin") {
    val query1 = parse(query,
      """
      SELECT * FROM t1 INNER JOIN t2 USING c1 INNER JOIN t3 USING c3
      WHERE t1.c1 = 100 AND t1.c2 = t2.c2 AND t1.c1 = t3.c3 AND c4 > 2
      """
    ).get
    val result1 = optimizer.pushFiltersJoin(query1)
    val expect1 = JoinAnalysis.annotateWithAvailTableNames(
      filter(filter(join(
      filter(join(
        filter(t("t1"), "t1.c1 = 100"),
        t("t2"), "c1"
      ), "t1.c2 = t2.c2"),
      t("t3"), "c3"
      ), "t1.c1 = t3.c3"), "c4 > 2")
    )
    assert(result1.children.head === expect1)

    assert(optimizer.pushFiltersJoin(result1) === result1, "idempotency")
  }

  test("makeReorderFilter") {
    val query1 = parse(query,
      "SELECT * FROM t WHERE t.c1 = 100 AND c2 > 2 AND sums(c3) > 2 and c4 IS NOT NULL"
    ).get
    val result1 = optimizer.makeReorderFilter(query1)
    val expect1 = rfilter(
      rfilter(rfilter(t("t"), "t.c1 = 100 AND c2 > 2"), "sums(c3) > 2"), "c4 IS NOT NULL"
    )
    assert(result1.children.head === expect1)

    val query2 = parse(query, "SELECT * FROM t WHERE sums(c1) > 2 AND c4 = 2").get
    val result2 = optimizer.makeReorderFilter(query2)
    val expect2 = rfilter(rfilter(t("t"), "sums(c1) > 2"), "c4 = 2")
    assert(result2.children.head === expect2)

    val query3 = parse(query, "SELECT * FROM t WHERE c4 = 2 AND c1 <2 AND sums(c2) > 2").get
    val result3 = optimizer.makeReorderFilter(query3)
    val expect4 = rfilter(rfilter(t("t"), "c4 = 2 AND c1 <2"), "sums(c2) > 2")
    assert(result3.children.head === expect4)

    assert(
      List(result1, result2, result3).forall(q => optimizer.makeReorderFilter(q) === q),
      "idempotency"
    )
  }

  test("sortToSortCols") {
    def getColSortBy(r: RelAlg): RelAlg =
      r.findp { case ColSortBy(_, _, _, _) => true }.get

    val query1 = parse(query,
      "SELECT c1, c2 * sums(c4 * c3), sum(c7) FROM t ASSUMING ASC c2 WHERE sums(c5) > 2 AND c6 < 2"
    ).get
    val result1 = optimizer.sortToSortCols(query1)
    // c7 is also sorted, as we need to sort before we filter
    val expect1 = scols(t("t"), "ASC c2", "c1, c2, c4, c3, c5, c6, c7")
    assert(getColSortBy(result1) === expect1)

    val query2 = parse(query,
      "SELECT c1, t.c2 * sum(c4 * o.c2) FROM t, other o ASSUMING ASC c2"
    ).get
    val result2 = optimizer.sortToSortCols(query2)
    val expect2 = scols(cross(t("t"), t("other as o")), "ASC c2", "c1, t.c2")
    assert(getColSortBy(result2) === expect2)

    val query3 = parse(query, "select * from t assuming asc c2").get
    val result3 = optimizer.sortToSortCols(query3)
    val expect3 = scols(t("t"), "ASC c2", "*")
    assert(getColSortBy(result3) === expect3)

    val query4 = parse(query, "select count(*) from t assuming asc c2").get
    val result4 = optimizer.sortToSortCols(query4)
    val expect4 = parse(query, "select count(*) from t").get
    assert(result4 === expect4)

    val query5 = filter(s(t("t"), "asc c2"), "c1 > 2 AND sum(c3)")
    val result5 = optimizer.sortToSortCols(query5)
    // sort all columns used as filter sits above sort
    val expect5 = scols(t("t"), "asc c2", "c1, c3")
    assert(getColSortBy(result5) === expect5)


    assert(
      List(result1, result2, result3, result4, result5).forall(q => optimizer.sortToSortCols(q) === q),
      "idempotency"
    )
  }

  test("batch optimization") {
    val prog1 = parse(program,
      """
        FUNCTION add(x, y) { x + y }
        FUNCTION myAvg(x) { sum(x) / count(x) }

        WITH
          temp1 as (
            SELECT c1, c2, sum(c3 * maxs(c0)), sum(c8) as myCol FROM
            some_t ASSUMING ASC c2, ASC c1 WHERE c2 > 2 AND sums(c6) > 2
            GROUP by c2
          )

           SELECT c1, add(c2, myCol), myAvg(c0)
           FROM
           temp1 INNER JOIN t USING c1 ASSUMING ASC c1, ASC t.c2
           WHERE t.c3 > 2 AND temp1.c0 > 2 AND add(t.c1, c4) > 2
           GROUP by c1
      """
    ).get
    val optimizer1 = BasicOptimizer(prog1)
    val result1 = optimizer1.optimize
    val funs = parse(program,
      """
         FUNCTION add(x, y) { x + y }
         FUNCTION myAvg(x) { sum(x) / count(x) }
      """
    ).get
    val temp1 =
      p(g(rfilter(
        scols(rfilter(t("some_t"), "c2 > 2"), "ASC c2, ASC c1", "c6,c1, c2, c3, c0, c8"),
        "sums(c6) > 2"
      ), "c2"), "c1, c2, sum(c3 * maxs(c0)), sum(c8) as myCol")

    val main1 =
      p(
          scols(g(rfilter(
          join(rfilter(t("temp1"), "temp1.c0 > 2"), rfilter(t("t"), "t.c3 > 2"), "c1"),
          "add(t.c1, c4) > 2"
        ), "c1"), "asc t.c2", "c1, c2, myCol"),
        "c1, add(c2, myCol), myAvg(c0)"
      )
    val q1 = Query(("temp1", Nil, temp1) :: Nil, main1)
    val expect1 = funs ++ List(q1)
    assert(clearAttr(result1) === clearAttr(expect1))
    // TODO adding more tests here is not a bad idea
  }
}
