package edu.nyu.aquery

import java.io.{File, PrintStream}

import scala.annotation.tailrec
import scala.io.Source

import edu.nyu.aquery.parse.AqueryParser
import edu.nyu.aquery.ast.Dot
import edu.nyu.aquery.analysis.TypeChecker

/**
 * AQuery executable. Takes various command line arguments.
 */
object Aquery extends App {
  def help(): Unit = {
    val msg =
      """
        AQuery Compiler
        Usage: -p  -c [-a #] [-s] [-o <file-name>] <file>
        -p: print dot graph to stdout
        -c: generated code
        -a: optimization level
        -s: silence warnings
        -tc: (soft) type checking (off by default)
        -o: code output file (if none, then stdout)
        If both -p and -c are set, will only perform last specified
      """.split("\n").map(_.trim).filter(_.length > 0).mkString("\n")
    println(msg)
  }

  abstract class CompilerActions

  case object Graph extends CompilerActions

  case object Compile extends CompilerActions

  case class AqueryConfig(
    action: CompilerActions = Compile,
    optim: Int = 0,
    silence: Boolean = false,
    typeCheck: Boolean = false,
    input: Option[String] = None,
    output: Option[String] = None)

  @tailrec
  def parseConfig(config: Option[AqueryConfig], args: List[String])
  : Option[AqueryConfig] = (config, args) match {
    case (c, Nil) => c
    case (None, _) => None
    case (c, "-p" :: rest) => parseConfig(c.map(_.copy(action = Graph)), rest)
    case (c, "-c" :: rest) => parseConfig(c.map(_.copy(action = Compile)), rest)
    case (c, "-a" :: level :: rest) if level forall Character.isDigit =>
      parseConfig(c.map(_.copy(optim = level.toInt)), rest)
    case (c, "-o" :: f :: rest) if f(0) != '-' => parseConfig(c.map(_.copy(output = Some(f))), rest)
    case (c, "-h" :: _) => None
    case (c, "-s" :: rest) => parseConfig(c.map(_.copy(silence = true)), rest)
    case (c, "-tc" :: rest) => parseConfig(c.map(_.copy(typeCheck = true)), rest)
    case (c, f :: Nil) if f(0) != '-' => c.map(_.copy(input = Some(f)))
    case _ => None
  }

  // Configuration -------------------------------------------------------------

  val defaultConfig: Option[AqueryConfig] = Some(AqueryConfig())
  val config = parseConfig(defaultConfig, args.toList).getOrElse {
    println(help())
    System.exit(1)
    AqueryConfig()
  }

  // Parsing -------------------------------------------------------------

  val inFile = config.input.map(Source.fromFile).getOrElse(Source.stdin)
  val contents = inFile.getLines().mkString("\n")
  inFile.close()

  val parsed = AqueryParser(contents) match {
    case AqueryParser.Success(prog, _) => Some(prog)
    case err@AqueryParser.Error(_, _) =>
      println(err.toString)
      None
    case err@AqueryParser.Failure(_, _) =>
      println(err.toString)
      None
  }

  // (Soft) Type Checking  ---------------------------------------------------
  val typeErrors = for (prog <- parsed if config.typeCheck) yield TypeChecker(prog)

  typeErrors.foreach { errs =>
    errs.sortBy(e => (e.pos.line, e.pos.column)).foreach(println)
    if (errs.nonEmpty) System.exit(1)
  }

  // Action -------------------------------------------------------------
  val representation = parsed.map { p =>
    config.action match {
      case Graph => Dot.toGraph(p)
      case Compile => p.toString
    }
  }

  // Output -------------------------------------------------------------
  val outFile = config.output.map(f => new PrintStream(new File(f))).getOrElse(System.out)
  representation.foreach(outFile.print)
  outFile.close()
}
