import edu.nyu.aquery.ast._
import edu.nyu.aquery.parse.AqueryParser
import edu.nyu.aquery.parse.AqueryParser._

import scala.collection.generic.SeqFactory
import scala.language.implicitConversions
import scala.io.Source
import org.scalatest.FunSuite

class ParserTestSuite extends FunSuite {
  // implicit conversions for convenience
  implicit def asExpr(i: Int): Expr = IntLit(i)

  implicit def asExpr(f: Float): Expr = FloatLit(f)

  implicit def asExpr(b: Boolean): Expr = BooleanLit(b)

  implicit def asExpr(s: String): Expr = StringLit(s)

  implicit def asExpr(c: Char): Expr = Id(c.toString)

  // some convenience wrappers
  def makeBinExpr(op: BinOp): (Expr, Expr) => BinExpr = (l, r) => BinExpr(op, l, r)
  val plus = makeBinExpr(Plus)
  val times = makeBinExpr(Times)
  val div = makeBinExpr(Div)
  val gt = makeBinExpr(Gt)
  val lt = makeBinExpr(Lt)


  /**
   * Convenience function to validate parse resukts
   * @param p parser
   * @param s string to parse
   * @param f optional predicate to test. If None, assumes parsing should fail
   * @tparam A
   * @return true if parsed succesfully and fits to predicate, true if fails to parse
   *         and predicate is None, false otherwise
   */
  def expectParse[A](p: Parser[A], s: String)(f: Option[A => Boolean]): Boolean = {
    val parsed = parse(p, s)
    f.map { fun =>
      parsed match {
        case Success(v, _) => fun(v)
        case _ => false
      }
    }.getOrElse(parsed.isEmpty)
  }

  def parseResource(resource: String): Boolean = {
    val prog = Source.fromURL(getClass.getResource(resource)).getLines.mkString("\n")
    AqueryParser(prog) match {
      case Success(_, _) => true
      case Failure(msg, _) =>
        println(msg)
        false
      case Error(msg, _) =>
        println(msg)
        false
    }
  }

  // expressions
  test("arithmetic") {
    // positive tests
    assert(
      expectParse(expr, "2 * 3 + 4 / 5 + 7")(Some(plus(plus(times(2, 3), div(4, 5)), 7) == _)),
      "should parse with precedence and left associative"
    )
    assert(
      expectParse(expr, "f(g(1,2,3), h()) + (3 + 4)")(
        Some(
          plus(FunCall("f", List(FunCall("g", List(1, 2, 3)), FunCall("h", Nil))), plus(3, 4)) == _
        )),
      "should parse functions with function args, parentheses should change associativity"
    )

    // negative tests
    assert(expectParse(expr, "2 / +3")(None), "should not parse")
  }

  test("case expressions") {
    // positive tests
    // simple cases
    val simpleS = "CASE WHEN x > 2 THEN 3 WHEN x < 0 THEN 4 * 10 END"
    val simpleE = Case(None, List(IfThen(gt('x', 2), 3), IfThen(lt('x', 0), times(4, 10))), None)

    val elseS = "CASE WHEN x > 2 THEN 3 WHEN x < 0 THEN 4 * 10 ELSE f() END"
    val elseE = simpleE.copy(e = Some(FunCall("f", Nil)))

    assert(expectParse(expr, simpleS)(Some(simpleE == _)), "simple without else")
    assert(expectParse(expr, elseS)(Some(elseE == _)), "simple with else")

    val simple2S = "case f(x) WHEN 3 THEN 4 ELSE 5 END"
    val simple2E = Case(Some(FunCall("f", List('x'))), List(IfThen(3, 4)), Some(5))
    assert(expectParse(expr, simple2S)(Some(simple2E == _)), "simple with initial expr")

    // nested
    val nestedS = "case g() WHEN 1 THEN (case WHEN y > 0 then 0 END) END"
    val nestedE = Case(
        Some(FunCall("g", Nil)),
        IfThen(1, Case(None, IfThen(gt('y', 0), 0) :: Nil, None)) :: Nil,
        None)
    assert(expectParse(expr, nestedS)(Some(nestedE == _)), "nested case expression")

    // negative tests
    assert(expectParse(expr, "CASE WHEN x > 2 THEN 3 ")(None), "missing end")
    assert(expectParse(expr, "CASE x >2 THEN 3")(None), "missing then")
  }

  test("create/insert") {
    val c1S = "CREATE TABLE t(a INT, b STRING)"
    val c1E = Create("t", Left(List(("a", TypeInt), ("b", TypeString))))
    assert(expectParse(create, c1S)(Some(c1E == _)), "create with schema")

    val c2S = "CREATE TABLE t as SELECT * FROM t1"
    val q = Query(Nil, Project(Table("t1", None), (WildCard, None) :: Nil))
    val c2E = Create("t", Right(q))
    assert(expectParse(create, c2S)(Some(c2E == _)), "create with query")
  }

  test("io") {
    val saveS = """ SELECT * FROM t INTO OUTFILE "my_test_file.csv" FIELDS TERMINATED BY "," """
    val saveE =
      Save("my_test_file.csv", Query(Nil, Project(Table("t"), (WildCard, None) :: Nil)), ",")
    assert(expectParse(io, saveS)(Some(saveE == _)))

    val loadS = """ LOAD DATA INFILE "my_test_file.csv" INTO TABLE t2 FIELDS TERMINATED BY "," """
    val loadE = Load("my_test_file.csv", "t2", ",")
    assert(expectParse(io, loadS)(Some(loadE == _)))
  }

  test("update") {
    val uS = "UPDATE t SET c1 = 10 ASSUMING ASC c WHERE a > 10 GROUP BY h HAVING sums(a) > 10"
    val uE = Update(
      "t",
      ("c1", IntLit(10)) :: Nil,
      (Asc, Id("c")) :: Nil,
      gt('a', 10) :: Nil,
      'h' :: Nil,
      gt(FunCall("SUMS", 'a' :: Nil), 10) :: Nil
    )
    assert(expectParse(update, uS)(Some(uE == _)), "parse update")
  }

  test("delete") {
    // row-wise
    val dRS =
      """
         DELETE FROM t
         ASSUMING ASC c1
         WHERE sums(c) > 100
         GROUP BY a HAVING max(d) > 20
      """
    val dRE = Delete(
      "t",
      Right(gt(FunCall("SUMS", 'c' :: Nil), 100) :: Nil),
      (Asc, Id("c1")) :: Nil,
      'a' :: Nil,
      gt(FunCall("MAX", 'd' :: Nil), 20) :: Nil
    )
    assert(expectParse(delete, dRS)(Some(dRE == _)), "row-wise deletion")

    // column-wise
    val dCS = "DELETE c1, c2 from t"
    val dCE = Delete("t", Left(Id("c1") :: Id("c2") :: Nil), Nil, Nil, Nil)
    assert(expectParse(delete, dCS)(Some(dCE == _)), "column-wise deletion")

    assert(expectParse(delete, "DELETE c1 FROM t WHERE c1 > 100")(None), "can't mix col-row delete")
  }


  // udfs
  test("udf") {
    // positive tests
    assert(expectParse(udf, "FUNCTION f(){2}")(Some(UDF("f", Nil, List(Right(2))) == _)), "empty function")
    assert(expectParse(udf, "FUNCTION g(x, y){2}")(Some(UDF("g", List("x","y"), List(Right(2))) == _)), "with args")
    assert(expectParse(udf, "FUNCTION f(x){ a := 10; a * 100 }")
      (Some(UDF("f", List("x"), Left(Assign("a", 10)) :: Right(times('a', 100)) :: Nil) == _)),
      "full function"
    )
    // negative tests
    assert(expectParse(udf, "FUNCTION f(){SELECT * FROM t}")(None), "invalid expression in body")
  }

  // joins
  // Given joins etc testing from clause separately is fairly important
  test("from-clause") {
    val from1S = "table1 as t1 INNER JOIN table2 t2 USING c, table3 "
    val from1E =
      Join(
        Cross,
        Join(
          InnerJoinUsing,
          Table("table1", Some("t1")),
          Table("table2", Some("t2")),
          List('c')
        ),
        Table("table3", None),
        Nil
      )
    assert(expectParse(from, from1S)(Some(from1E == _)), "left associative joins, multiple aliasing")

    val from2S = "g(),t1,t2,other as ot"
    val from2E =
      List(
        BottomApply("g", Nil),
        Table("t1", None),
        Table("t2", None),
        Table("other", Some("ot"))
      ).reduceLeft((x, y) => Join(Cross, x, y, Nil))
    assert(expectParse(from, from2S)(Some(from2E == _)), "left associative crosses")

    // negative tests
    assert(expectParse(from, "g() as m")(None), "cannot alias bottom apply")
  }

  test("complete query") {
    val q1S = """
      SELECT sum(a) as sumA
      FROM t
      ASSUMING ASC c1, ASC c2
      WHERE a > 100 AND b < 20
      GROUP BY c * 10 as ced HAVING count(d) > 10
    """
    val p1 = Project(_: RelAlg, List((FunCall("SUM", 'a' :: Nil), Some("sumA"))))
    val s1 = SortBy(_: RelAlg, (Asc, Id("c1")) :: (Asc, Id("c2")) :: Nil)
    val g1 = GroupBy(_: RelAlg, List((times('c', 10), Some("ced"))), List(gt(FunCall("COUNT", 'd' :: Nil), 10)))
    val w1 = Filter(_: RelAlg, List(gt('a', 100), lt('b', 20)))

    val q1E = (s1 andThen w1 andThen g1 andThen p1)(Table("t", None))
    assert(expectParse(query, q1S)(Some(q1E == _)), "parsed simple query with all components")
  }

  // full programs
  // use most/all constructs
  test("simple program") {
    assert(parseResource("simple.a"), "simple program code parses")
  }

  test("fintime benchmark") {
    assert(parseResource("fintime.a"), "fintime benchmark code parses")
  }

  test("monetbd benchmark") {
    assert(parseResource("monetdb.a"), "monetdb benchmark code parses")
  }

  test("pandas benchmark") {
    assert(parseResource("pandas.a"), "pandas benchmark code parses")
  }
}
