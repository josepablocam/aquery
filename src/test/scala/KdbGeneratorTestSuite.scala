import edu.nyu.aquery.Aquery

import java.io.{File, PrintWriter}
import java.io.File.createTempFile

import org.scalatest.FunSuite

import scala.sys.process._

/**
 * All tests here focus on making sure the result of running the code is the same as if
 * we wrote equivalent q code. We don't test the exact form of the code generated, as this
 * is likely to change throughout time
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
    Aquery.main(Array("-a", optCode, "-c", "-o", tfile.getAbsolutePath, afile.getAbsolutePath))
    // want translated and kdb file
    (tfile, qfile)
  }

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
        CREATE TABLE aq_t2
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

  test("update/delete") {
    val acode =
      """
        <q> base:([]c1:1000?100; c2:1000?100; c3:1000?100); </q>

        CREATE TABLE upd_t
          SELECT * FROM base

        CREATE TABLE del_t
          SELECT * FROM base

         UPDATE upd_t
         SET c1 = c1 * 2, c3 = CASE WHEN c3 > 50 THEN 1 else -1 END
         ASSUMING ASC c1

         DELETE FROM del_t GROUP BY c1 HAVING COUNT(c2) > 4

        <q>.aq.c1:{upd_t}; .aq.c2:{del_t};</q>
      """
    val qcode =
      """
        .kdb.c1:{0N!update c1:c1 * 2, c3:?[c3 > 50;1;-1] from `c1 xasc base};
        .kdb.c2:{0N!delete from base where 4 <(count;c2) fby c1}
      """

    val tests = "c1, c2"
    val (passed0, msg0) = run(acode, qcode, tests, optimize = false)
    assert(passed0, "basic: " + msg0)

    val (passed1, msg1) = run(acode, qcode, tests, optimize = true)
    assert(passed1, "optimized: " + msg1)
  }

  test("simple queries") {
    val acode =
      """
        <q> base:([]c1:1000?100; c2:1000?100; c3:1000?100); </q>

        CREATE TABLE upd_t
          SELECT * FROM base

        CREATE TABLE del_t
          SELECT * FROM base

         UPDATE upd_t
         SET c1 = c1 * 2, c3 = CASE WHEN c3 > 50 THEN 1 else -1 END
         ASSUMING ASC c1

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

  ignore("simple.a") {
    // TODO
  }

  ignore("fintime.a") {
  // TODO
  }

  ignore("monetdb.a") {
  // TODO
  }

  ignore("pandas.a") {
  // TODO
  }
}
