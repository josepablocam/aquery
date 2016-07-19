import edu.nyu.aquery.analysis.OrderAnalysis
import edu.nyu.aquery.ast._
import edu.nyu.aquery.parse.AqueryParser._

import org.scalatest.FunSuite


class OrderAnalysisTestSuite extends FunSuite {
  // analyzer with no context beyond built-ins
  val simple = OrderAnalysis()

  // collecting info for udfs and using in body
  val functions =
    parse(program,
      """
        FUNCTION f(x) { z := 2 ; max(sums(z) * 2) }
        FUNCTION g() { res := f(1); res + 3 }
        FUNCTION h(x) { res := 2 * sum(x); res ^ 3 }
        FUNCTION add(x, y) { x + y }
        FUNCTION myMax(x) { max(x) }
      """
    ).get
  val withContext = OrderAnalysis(functions)

  test("order-dependence") {
    // expressions
    val hasOd1 = parse(expr, "c1 * sum(c1 + max(sums(c2)))").get
    assert(simple.hasOrderDependence(hasOd1), "uses sums")
    val hasOd2 = parse(expr, "c1 + f(1)").get
    assert(simple.hasOrderDependence(hasOd2), "uses unknown function")
    val noOd = parse(expr, "c1 + max(c4 * abs(-1))").get
    assert(!simple.hasOrderDependence(noOd), "uses no order-dependent functions")

    // udfs
    val hasOd3 = parse(udf, "FUNCTION f(x) { z := 2; max(sums(z) * 2) }").get
    assert(simple.hasOrderDependence(hasOd3), "uses sums in body")
    val hasOd4 = parse(udf, "FUNCTION f(x) { z := g(2); z * 3 }").get
    assert(simple.hasOrderDependence(hasOd4), "uses unknown function in local var assignment")

    // with context of existing functions
    val noOd2 = parse(expr, "c1 * max(100, h())").get
    assert(!withContext.hasOrderDependence(noOd2), "all functions used are order independent")
    val hasOd5 = parse(expr, "c1 + min(100, 10, g())").get
    assert(withContext.hasOrderDependence(hasOd5), "order-dependence through transitivity")
    val hasOd6 = parse(expr, "c0 * f(max(h()))").get
    assert(withContext.hasOrderDependence(hasOd6), "order-dependent f")
  }

  test("removing order dependence") {
    val rem1 = parse(udf, "FUNCTION f(x, y) { z := max(sums(x)); z + 2 }").get
    assert(simple.hasOrderDependence(rem1), "uses sums")
    assert(simple.removesOrderDependence(rem1), "final result is sum and integer addition")

    val noRem1 = parse(udf, "FUNCTION f(x, y) { z := x + sum(y); x * z }").get
    assert(!simple.hasOrderDependence(noRem1), "no order dependence")
    assert(!simple.removesOrderDependence(noRem1), "multiplies by param")

    val noRem2 = parse(udf, "FUNCTION f(x, y) { x + y * x }").get
    assert(!simple.hasOrderDependence(noRem2), "no order dependence")
    assert(!simple.removesOrderDependence(noRem2), "simple arithmetic")

    // using the environment
    val rem2 = parse(udf, "FUNCTION myFun(x) { h(x) + sum(x) }").get
    assert(withContext.removesOrderDependence(rem2), "h removes order dep, and sum too")

    val rem3 = parse(udf, "FUNCTION myFun(x) { g() }").get
    assert(withContext.hasOrderDependence(rem3), "g calls f, f has sums")
    assert(withContext.removesOrderDependence(rem3), "= f (rem) + integer add")
  }

  test("columns w/ direct sorting") {
    val call1 = parse(expr, "c2 + c4 * sums(c1 * c2) + max(c6, f(c4, t.c5, sum(t8)))").get
    val expect1 = Set(Id("c2"), Id("c1"), Id("c4"), ColumnAccess("t", "c5"))
    val result1 = simple.directOrderDependentCols(call1)
    assert(result1 === expect1, "cols in sums and f")

    val call2 = parse(expr, "c5 * h(c1) + sums(c2)").get
    val expect2 = Set(Id("c2"))
    val result2 = withContext.directOrderDependentCols(call2)
    assert(result2 === expect2, "cols in sums only")

    val call3 = parse(expr, "h(c1, sums(c2 + max(c4))) * f(c0)").get
    val expect3 = Set("c2", "c0").map(Id)
    val result3 = withContext.directOrderDependentCols(call3)
    assert(result3 === expect3, "sums and f args")

    val call4 = parse(expr, "max(c2 + f(c4 * count(c5)))").get
    val expect4 = Set(Id("c4"))
    val result4 = withContext.directOrderDependentCols(call4)
    assert(result4 === expect4, "f arg")
  }

  test("interaction sets") {
    val e1 = parse(expr, "c1 + c2 + max(c3 + sums(c4), c5)").get
    val expect1 = Set(Set("c1", "c2"), Set("c3", "c4", "c5")).map(_.map(Id))
    val result1 = simple.collectInteractions(e1)
    assert(result1 === expect1)

    val e2 = parse(expr, "c1 + c2 + max(c3 + sums(c4), c5 * h(c6))").get
    val expect2 = Set(Set("c1", "c2"), Set("c3", "c4", "c5"), Set("c6")).map(_.map(Id))
    val result2 = withContext.collectInteractions(e2)
    assert(result2 === expect2)

    val e3 = parse(expr, "max(c3, sum(c4 + c5)) * c0 + sums(c6)").get
    val expect3 = Set(Set("c3"), Set("c4", "c5"), Set("c0", "c6")).map(_.map(Id))
    val result3 = simple.collectInteractions(e3)
    assert(result3 === expect3)

    val e4 = parse(expr,
      """
        CASE maxs(c4) > c5
          WHEN TRUE THEN sum(c6) * c7
          ELSE 100
         END
      """).get
    val expect4 = Set(Set("c4", "c5", "c7"), Set("c6")).map(_.map(Id))
    val result4 = simple.collectInteractions(e4)
    assert(result4 === expect4)
  }

  test("interactions indirectly reachable") {
    // nasty type casts due to set invariance
    val interact1 = Set(Set("c4", "c5", "c7"), Set("c6", "c7")).map(_.map(Id(_).asInstanceOf[Expr]))
    val seed1 = Set[Expr](Id("c5"))
    val expect1 = Set("c4", "c6", "c7").map(Id(_).asInstanceOf[Expr])
    val result1 = simple.indirectReachable(interact1, seed1)
    assert(result1 === expect1)

    val interact2 =  Set(Set("c4", "c5"), Set("c5", "c6"), Set("c6"), Set("c7")).map(_.map(Id(_).asInstanceOf[Expr]))
    val seed2 = Set[Expr](Id("c7"))
    val expect2 = Set()
    val result2 = simple.indirectReachable(interact2, seed2)
    assert(result2 === expect2)

    val seed3 = Set[Expr](Id("c6"))
    val expect3 = Set[Expr](Id("c4"),  Id("c5"))
    val result3 = simple.indirectReachable(interact2, seed3)
    assert(result3 === expect3)

    val seed4 = Set[Expr](Id("c6"))
    val interact4: Set[Set[Expr]] =  Set(
      Set(Id("c4"), Id("c5")),
      Set(ColumnAccess("t", "c5"), Id("c6")),
      Set(Id("c6")),
      Set(Id("c7"))
    )
    val expect4 = Set[Expr](Id("c4"),  Id("c5"), ColumnAccess("t", "c5"))
    val result4 = simple.indirectReachable(interact4, seed4)
    assert(result4 === expect4)


    val seed5 = Set[Expr](Id("c6"))
    val interact5: Set[Set[Expr]] =  Set(
      Set(Id("c4"), ColumnAccess("t1","c5")),
      Set(ColumnAccess("t", "c5"), Id("c6")),
      Set(Id("c6")),
      Set(Id("c7"))
    )
    val expect5 = Set[Expr](ColumnAccess("t", "c5"))
    val result5 = simple.indirectReachable(interact5, seed5)
    assert(result5 === expect5)
  }

  test("minimal sorting cols") {
    val e1 = parse(expr, "c1 + sums(c4 * max(c5))").get
    // c4 due to sums, c1 indirect
    val expect1 = Set("c1", "c4").map(Id)
    val result1 = simple.colsToSort(List(e1), collectAtRoot = true)
    assert(result1 === expect1)

    val e2 = e1
    // c4, as c1 no longer collected at root
    val expect2 = Set("c4")
    val result2 = simple.colsToSort(List(e1), collectAtRoot = false)

    val e3 = parse(expr, "max(f(c1, c2) + sums(c3))").get
    // c1 and c2 due to f, c3 due to sums
    val expect3 = Set("c1", "c2", "c3").map(Id)
    val result3 = withContext.colsToSort(List(e3), collectAtRoot = true)
    assert(result3 === expect3)

    val e4 = parse(expr, "sums(c1 + max(c4 * f(c0)) + myMax(c4)) + sum(c1 * c2)").get
    // c1 due to sums, c0 due to f (c4 not necessary, as f is order dependence remover)
    // c2 due to indirection (through c1)
    val expect4 = Set("c1", "c0", "c2").map(Id)
    val result4 = withContext.colsToSort(List(e4), collectAtRoot = true)
    assert(result4 === expect4)

    val e5 = parse(expr, "c0 + max(c1) * CASE sums(c5) > 1 WHEN TRUE THEN c2 ELSE 100 END").get
    // c0 at root, c5 due to sums, c2 due to indirection (through c5)
    val expect5 = Set("c0", "c5", "c2").map(Id)
    val result5 = withContext.colsToSort(List(e5), collectAtRoot = true)
    assert(result5 === expect5)

    val e6 = parse(repsep(expr, ";"),
      """
         c0 * sums(c1) ;
         max(c1 + c2) ;
         sum(c3) + c0 ;
         c5 / f(c0 + h(100 + c6))
      """).get
    // c0 due to indirection (through c1) and due to f
    // c1 due to sums
    // c2 due to indirection (through c1)
    // c5 due to root
    val expect6 = Set("c0", "c1", "c2", "c5").map(Id)
    val result6 = withContext.colsToSort(e6, collectAtRoot = true)
    assert(result6 === expect6)

    val e7 = parse(repsep(expr, ";"),
      """
         2 / h(c0 * sums(c5)) ;
         CASE c6 > 7
            WHEN TRUE THEN max(c7)
            ELSE c8
          END ;
         max(c6 / add(c7, 3)) * c8
      """).get
    // c0 through indirection (c5)
    // c5 due to sums
    // c6 due to root
    // c8 due to root (and indirection with c6)
    // c7 due to indirection
    val expect7 = Set("c0", "c5", "c6", "c7", "c8").map(Id)
    val result7 = withContext.colsToSort(e7, collectAtRoot = true)
    assert(result7 === expect7)


    val e8 = parse(repsep(expr, ";"),
      """
         c0 * sums(t.c1) ;
         max(other.c1 + c2) ;
         sum(c3) + c0 ;
         c5 / f(c0 + h(100 + c6))
      """).get
    // c0 due to indirection (through t.c1) and due to f
    // t.c1 due to sums
    // c5 due to root
    // note that c2 is no longer indirectly reachable as other.c1 != t.c1
    val expect8: Set[Expr] = Set(Id("c0"), ColumnAccess("t", "c1"), Id("c5"))
    val result8 = withContext.colsToSort(e8, collectAtRoot = true)
    assert(result8 === expect8)
  }

  test("has sort and extract sort") {
    val query1 = parse(query, "SELECT * FROM t ASSUMING ASC c1, DESC t.c2 WHERE c1 > 2").get
    assert(simple.hasSortBy(query1))
    assert(simple.getSort(query1) === List((Asc, Id("c1")), (Desc, ColumnAccess("t", "c2"))))

    val query2 = parse(query, "SELECT * FROM t WHERE c1 > 2").get
    assert(!simple.hasSortBy(query2))
    assert(simple.getSort(query2) === List())

    val query3 = parse(query,
      "SELECT * FROM t, t1 INNER JOIN t2 USING c1 ASSUMING ASC c1, DESC t.c2 WHERE c1 > 2"
    ).get
    assert(simple.hasSortBy(query3))
    assert(simple.getSort(query3) === List((Asc, Id("c1")), (Desc, ColumnAccess("t", "c2"))))
  }
}
