import edu.nyu.aquery.Aquery

import java.io.{File, PrintWriter}
import java.io.File.createTempFile

import org.scalatest.{Ignore, FunSuite}

import scala.io.Source
import scala.sys.process._

/**
 * All tests here focus on making sure the result of running the code is the same as if
 * we wrote equivalent q code. We don't test the exact form of the code generated, as this
 * is likely to change throughout time.
 *
 * If a test requires any data or pre-defined functions, these should be added to the aquery code
 * (wrapped in <q> </q> if necessary), as this is run first in the q test harness (runner.q).
 *
 * To run these tests, you need q in your PATH, so that Scala can launch an external q process
 * for each test.
 */
class KdbGeneratorTestSuite extends FunSuite {
  // q script that wraps running tests
  val runner = getClass.getResource("q/runner.q").getFile

  // create the running command
  def cmd(afile: String, qfile: String, test: String): Seq[String] =
    List("q", runner, "-aquery", afile, "-kdb", qfile, "-test", test)

  // trim code to avoid issues on the q side
  def cleanCode(str: String): String =
    str.split("\n").map { l =>
      val t = l.trim
      if (t.startsWith(".")) t else " " + t
    }.mkString("\n")

  // write code to files, translate in case of aquery
  def toFiles(acode: String, qcode: String, optimize: Boolean): (File, File) = {
    val afile = createTempFile("aquery_", ".a")
    val tfile = createTempFile("translated_", ".q")
    val qfile = createTempFile("kdb_", ".q")

    val awriter = new PrintWriter(afile)
    val qwriter = new PrintWriter(qfile)

    awriter.write(cleanCode(acode))
    awriter.close()

    qwriter.write(cleanCode(qcode))
    qwriter.close()

    // translate
    val optCode = if (optimize) "1" else "0"
    // don't immmediately execute queries (runner.q calls them as necessary)
    Aquery.main(Array("-nr", "-a", optCode, "-c", "-o", tfile.getAbsolutePath, afile.getAbsolutePath))
    // want translated and kdb file
    (tfile, qfile)
  }

  // read code from a file
  def getCode(f: String): String = {
    val file = getClass.getResource(f).getFile
    Source.fromFile(file).getLines().mkString("\n")
  }

  // run code using external q process
  def run(acode: String, qcode: String, test: String, optimize: Boolean): (Boolean, String)= {
    val (afile, qfile) = toFiles(acode, qcode, optimize)
    val c = cmd(afile.getAbsolutePath, qfile.getAbsolutePath, test)
    val stdout = new StringBuffer
    val success = c.run(BasicIO(withIn = false, stdout, None)).exitValue() == 0
    if (success) {
      afile.deleteOnExit()
      qfile.deleteOnExit()
    }
    (success, "\n" + stdout.toString)
  }

  test("simple expressions/UDF") {
    val acode =
      """
        FUNCTION f(){
          x := 100 * 2 - 3;
          y := x ^ 3;
          l := LIST(1,2,3,4,5,6);
          z :=
            CASE l
              WHEN 2 THEN -1
              WHEN 3 THEN 100
              ELSE 200
            END;
          w := sums(2, l);
          LIST(sqrt(y), z, w)}
          <q> .aq.f:f; </q>
      """
    val qcode =
      """
        .kdb.f:{[]
          x:-3+100*2;
          y:x xexp 3;
          l:1 2 3 4 5 6;
          z:200^(2 3!-1 100) l;
          w:2 msum l;
          (sqrt y; z; w)};
      """

    val tests = "f"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: " + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }

  test("create") {
    val acode =
      """
        <q> base:([]c1:1000?100; c2:1000?100; c3:1000?100) </q>
        CREATE TABLE aq_t1 (c1 INT, c2 STRING, c3 BOOLEAN)
        CREATE TABLE aq_t2 AS
          SELECT
          c1 * 2 as c1, sums(c2) as c2, max(c3) as max_c3
          FROM base ASSUMING ASC c3 WHERE c1 > 10
          GROUP BY c1

        <q>.aq.c1:{aq_t1}; .aq.c2:{aq_t2};</q>
      """
    val qcode =
      """
        .kdb.c1:{([]c1:`long$();c2:`$();c3:`boolean$())};
        .kdb.c2:{
          select c1:c1 * 2, c2, max_c3:c3 from select sums c2, max c3 by c1 from `c3 xasc base where c1 > 10
          }
      """

    val tests = "c1, c2"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: " + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }

  test("insert") {
    val acode =
      """
         <q> base:([]c2:1 2 3 4; c3:100 200 300 400) </q>
         CREATE TABLE t (c1 INT, c2 INT, c3 STRING)
         INSERT INTO t VALUES(1, 2, "c")
         INSERT INTO t VALUES(10, 20, "C")
         INSERT INTO t(c1, c2, c3)
          SELECT c2, c2, "this is a test" from base

         SELECT * FROM t
      """
    val qcode =
      """
        .kdb.q0:{
          t:([]c1:`long$(); c2:`long$(); c3:`$());
          t:t upsert (1;2;`c);
          t:t upsert (10;20;`C);
          t:t upsert select c1:c2, c2, c3:`$"this is a test" from base;
          t
          }
      """

    val tests = "q0"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: " + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }

  test("load/save") {
    val acode =
      """
         CREATE TABLE t (c1 INT, c2 INT, c3 STRING)
         INSERT INTO t VALUES(1, 2, "c")
         INSERT INTO t VALUES(10, 20, "C")

         SELECT * FROM t
         INTO OUTFILE "my_test_file.csv" FIELDS TERMINATED BY ","

         CREATE TABLE t2(c1 INT, c2 INT, c3 STRING)
         LOAD DATA INFILE "my_test_file.csv"
         INTO TABLE t2 FIELDS TERMINATED BY ","

        <q> .aq.q0:{t2}; system "rm -f my_test_file.csv" </q>
      """
    val qcode = ".kdb.q0:{t}"

    val tests = "q0"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: " + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }

  test("update/delete") {
    val acode =
      """
        <q> base:([]c1:1000?100; c2:1000?100; c3:1000?100); </q>

        CREATE TABLE upd_t as
          SELECT * FROM base

        CREATE TABLE del_t as
          SELECT * FROM base

         UPDATE upd_t ASSUMING ASC c1
         SET c1 = c1 * 2, c3 = CASE WHEN c3 > 50 THEN 1 else -1 END

         DELETE FROM del_t GROUP BY c1
         HAVING COUNT(c2) > 4 AND any(c3 > 2)

        <q>.aq.c1:{upd_t}; .aq.c2:{del_t};</q>
      """
    val qcode =
      """
        .kdb.c1:{update c1:c1 * 2, c3:?[c3 > 50;1;-1] from `c1 xasc base};
        .kdb.c2:{delete from base where ({(any bi[`c3]>2)&4<count bi:base[x]};i) fby c1}
      """

    val tests = "c1,c2"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: " + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }

  test("each-modifier") {
    val acode =
      """
        <q> base:([]c1:1000?100; c2:raze 100#'til 10; c3:1000?100); </q>

        WITH
          temp1 AS (
            SELECT c2, c1 FROM base GROUP BY c2
            )
         SELECT
         c2,
         EACHROW(sum(c1) * deltas(c1)) as random,
         EACHROW(max(c1)) as max_per_group
         from temp1

        WITH
         temp1 AS (
          SELECT c2, c1 FROM base GROUP BY c2
          )
          SELECT max(c1) as max_array from temp1
      """
    val qcode =
      """
        .kdb.q0:{
          0!select random:sum[c1] * deltas c1, max_per_group:max c1 by c2 from base
          }
         .kdb.q1:{ select max_array:max c1 from `c2 xgroup base }
      """

    val tests = "q0,q1"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: " + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }

  test("simple.a") {
    val acode = getCode("simple.a")
    val qcode = getCode("q/simple.q")

    val tests = "q0,q1,q2,q3,q4,q5,q6,q7,q8,q9"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, msg1)
  }

  test("fintime.a") {
    // need a bit more work here to get path to data
    val load = getCode("q/load_fintime.q")
    val dataPath = getClass.getResource("data/").getFile
    val fullLoad = s"""<q>\nDATAPATH:"$dataPath";\n$load\n</q>"""
    // extend aquery code with data loading
    val acode = fullLoad + "\n" + getCode("fintime.a")
    val qcode = getCode("q/fintime.q")

    val tests = "q0,q1,q2,q3,q4,q5,q6,q7,q8,q9"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, msg1)
  }

  test("monetdb.a") {
    val acode = getCode("monetdb.a")
    val qcode = getCode("q/monetdb.q")

    val tests = "q0,q1"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: "  + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }

  test("pandas.a") {
    val acode = getCode("pandas.a")
    val qcode = getCode("q/pandas.q")

    val tests = "q0,q1,q2,q3,q4,q5,q6"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: "  + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }
}
