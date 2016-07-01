package edu.nyu.aquery.parse

import java.text.SimpleDateFormat

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.Try

/**
 * Provides tokenization for AQuery specific tokens. Currently handles:
 * - Date parsing (validated for correctness of date)
 * - Timestamp parsing (validated for correctness of timestamp)
 * - Verbatim code markers and body within it
 */
trait AqueryTokens extends StdLexical {
  // New token classes
  case class Date(chars: String) extends Token
  case class Timestamp(chars: String) extends Token
  case class Verbatim(chars: String) extends Token

  // Formats for date and timestamp
  val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
  // TODO: currently handling nano-seconds in lexer...not nice...this should be all here
  val timestampFormat = new SimpleDateFormat("MM/dd/yyyy'D'HH:mm:ss")

  // make date-related parsers strict (i.e. fail on invalid date)
  dateFormat.setLenient(false)
  timestampFormat.setLenient(false)

  /**
   * Create a parser for a date/timestamp based on a pattern
   * @param pattern pattern
   * @param failMsg message to provide if parsing fails
   * @return
   */
  private def dateParser(pattern: SimpleDateFormat, failMsg: String): Parser[String] = new Parser[String] {
    def javaParse(text: CharSequence, offset: Int): (String, Int) = {
      // remove quotes used for literal letters in pattern
      val patternLength = pattern.toPattern.replace("'", "").length
      val endOffset = offset + patternLength
      // wrap operations in Try to avoid exceptions
      val dateText = Try(text.subSequence(offset, endOffset).toString)
      // parse date, make string appropriate format, and add offset information
      val parsed = dateText
        .map(s => pattern.parse(s))
        .map(d => pattern.format(d))
        .map(d => (d, endOffset))
      parsed.getOrElse(("", -1))
    }

    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      val start = offset
      val (parsed, endPos) = javaParse(source, start)
      if (endPos >= 0)
        Success(parsed, in.drop(endPos - offset))
      else
        Failure(failMsg, in.drop(start - offset))
    }
  }

  // date token parser
  protected def date = dateParser(dateFormat, "Failed to parse date") ^^ { d => Date(d) }
  // timestamp token parser
  protected def timestamp = dateParser(
    timestampFormat,
    "Failed to parse datetime") ^^ { ts => Timestamp(ts) }

  // verbatim code needs to be tokenized appropriately
  private def oVerbatim = '<' ~ 'q' ~ '>' ^^^ ""
  private def cVerbatim = '<' ~ '/' ~ 'q' ~ '>' ^^^ ""
  private def verbatimBodyChars = (
    chrExcept('<') ^^ {x => x.toString}
      | '<' ~ chrExcept('/') ^^ { case _ ~ c => "<" + c}
      | '<' ~ '/' ~ chrExcept('q') ^^ { case _ ~ _ ~ c => "</" + c }
      | '<' ~ '/' ~ 'q' ~ chrExcept('>') ^^ { case _ ~ _ ~ _ ~ c => "</q" + c }
    )
  protected def verbatim = oVerbatim ~> rep(verbatimBodyChars) <~ cVerbatim  ^^ {
    case chars => Verbatim(chars mkString "")
  }

}
