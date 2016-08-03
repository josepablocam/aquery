package edu.nyu.aquery

import java.io.{File, PrintStream}

import edu.nyu.aquery.codegen.KdbGenerator

import scala.annotation.tailrec
import scala.io.Source

import edu.nyu.aquery.ast.Dot
import edu.nyu.aquery.analysis.TypeChecker
import edu.nyu.aquery.optimization.BasicOptimizer
import edu.nyu.aquery.parse.AqueryParser



/**
 * AQuery executable. Takes various command line arguments.
 */
object Aquery extends App {
  def trimMsg(msg: String) = msg.split("\n").map(_.trim).filter(_.length > 0).mkString("\n")

  def help(): String = {
    trimMsg(
      """
        AQuery Compiler
        Options:
        -p: print dot graph
        -c: generate code
        -a: optimize (0/1)
        -opts: comma separated list of optimizations (set to all by default if not provided)
        -s: silence warnings
        -tc: (soft) type checking (off by default)
        -o: code output file (if none, then stdout)
        -nr: defines queries as functions, but doesn't automatically call them (useful for testing)
        If both -p and -c are set, will only perform last specified
      """
    )
  }

  def optsAvailable(): String = trimMsg(BasicOptimizer().description)

  abstract class CompilerActions

  case object Graph extends CompilerActions

  case object Compile extends CompilerActions

  case class AqueryConfig(
    action: CompilerActions = Compile, // end action to perform on input code
    optim: Int = 0,                    // optimize or not
    silence: Boolean = false,          // silence warnings
    typeCheck: Boolean = false,        // perform soft type checking
    optimFuns: Seq[String] = Nil,      // specific optimizations
    runQueries: Boolean = true,        // run queries immediately after defining
    input: Option[String] = None,      // input file
    output: Option[String] = None)     // output file

  def notCommand(s: String) = s(0) != '-'

  @tailrec
  def parseConfig(config: Option[AqueryConfig], args: List[String])
  : Option[AqueryConfig] = (config, args) match {
    case (c, Nil) => c
    case (None, _) => None
    case (c, "-p" :: rest) => parseConfig(c.map(_.copy(action = Graph)), rest)
    case (c, "-c" :: rest) => parseConfig(c.map(_.copy(action = Compile)), rest)
    case (c, "-a" :: level :: rest) if level forall Character.isDigit =>
      parseConfig(c.map(_.copy(optim = level.toInt)), rest)
    case (c, "-o" :: f :: rest) if notCommand(f) => parseConfig(c.map(_.copy(output = Some(f))), rest)
    case (c, "-h" :: _) => None
    case (c, "-s" :: rest) => parseConfig(c.map(_.copy(silence = true)), rest)
    case (c, "-tc" :: rest) => parseConfig(c.map(_.copy(typeCheck = true)), rest)
    case (c, f :: Nil) if notCommand(f) => c.map(_.copy(input = Some(f)))
    case (c, "-opts" :: opts :: rest) if notCommand(opts) =>
      parseConfig(c.map(_.copy(optimFuns = opts.split(","))), rest)
    case (c, "-nr" :: rest) => parseConfig(c.map(_.copy(runQueries = false)), rest)
    case _ => None
  }

  // Configuration -------------------------------------------------------------
  val defaultConfig: Option[AqueryConfig] = Some(AqueryConfig())
  val config = parseConfig(defaultConfig, args.toList).getOrElse {
    println(help())
    println()
    println(optsAvailable())
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
  val typeErrors = for (prog <- parsed if config.typeCheck) yield TypeChecker(prog).typeCheck(prog)

  typeErrors.foreach { errs =>
    errs.sortBy(e => (e.pos.line, e.pos.column)).foreach(println)
    if (errs.nonEmpty) System.exit(1)
  }

  // Optimization of plan
  val optimized = parsed.map { prog =>
    if (config.optim != 0)
      /// optimize
      BasicOptimizer(prog, config.optimFuns).optimize
    else
      prog
  }

  // Action -------------------------------------------------------------
  val representation = optimized.map { p =>
    config.action match {
      case Graph => Dot.toGraph(p)
      case Compile => KdbGenerator.generate(p, config.runQueries)
    }
  }

  // Output -------------------------------------------------------------
  val outFile = config.output.map(f => new PrintStream(new File(f))).getOrElse(System.out)
  representation.foreach(outFile.print)
  outFile.close()
}
