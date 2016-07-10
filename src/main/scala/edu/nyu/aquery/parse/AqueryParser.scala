package edu.nyu.aquery.parse

import scala.language.implicitConversions
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh

import edu.nyu.aquery.ast._

/**
 * Aquery grammar parser. The grammar is described using EBNF when possible. Packratparser
 * used to handle left-recursive productions automatically. Note that the order of productions
 * for a given non-terminal is significant, with longest-possible matches listed first.
 *
 * The implementation is loosely based on
 * https://github.com/scalan/scalan/blob/master/meta/src/main/scala/scalan/meta/SqlParser.scala,
 * mainly the way of handling keywords with reflection.
 */
object AqueryParser extends StandardTokenParsers with PackratParsers {
  // define a simple lexer overriding some basic tokenization
  class AqueryLexical(val keywords: Seq[Keyword]) extends AqueryTokens {
    override def token: Parser[Token] =
      ( identChar ~ rep( identChar | digit ) ^^ { case first ~ rest => processIdent(first :: rest mkString "") }
        // floats
        | rep1(digit) ~ '.' ~ rep(digit) ^^ { case first ~ d ~ rest => NumericLit(first ::: d :: rest mkString "") }
        // ints
        | digit ~ rep( digit ) ^^ { case first ~ rest => NumericLit(first :: rest mkString "") }
        // strings are only double quoted in AQuery, single quotes are used for dates/date-times
        | '\"' ~ rep( chrExcept('\"', '\n', EofCh) ) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
        // dates and timestamp are handled with single quotes
        // nano-seconds are handled separately and simply attached to string representation
        | '\'' ~> timestamp ~ (('.' ~> rep(digit)).? <~ '\'') ^^ { case ts ~ ns =>
          val nano = ns.map(n => "." + n.mkString("")).getOrElse("")
          StringLit(ts.chars + nano)
        }
        | '\'' ~> date <~ '\''
        | '\"' ~> failure("unclosed string literal")
        | '\'' ~> failure("unclosed/invalid date/timestamp")
        // verbatim before any delimiters to consume contents
        | verbatim
        | delim
        | EofCh ^^^ EOF
        | failure("illegal character")
        )

    // use canonical representation to handle keywords that must be uppercase
    reserved ++= keywords.flatMap(w => w.canonical)
    delimiters +=(
      "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", "==",
      ",", ";", "%", "{", "}", ":=", ":", "[", "]", ".", "&", "|", "^", "~", "<=>")
  }


  // there are some keywords that we always want to be upper case
  val alwaysUpperCase = Set("TIMESTAMP", "INT", "FLOAT", "STRING", "BOOLEAN", "DATE")

  // str: string representation, fun: keyword is a valid built-in function
  protected case class Keyword(str: String, fun: Boolean = false) {
    lazy val canonical =  List(str.toUpperCase) :::
      (if (!alwaysUpperCase.contains(str.toUpperCase)) List(str.toLowerCase) else Nil)

    lazy val parser: Parser[Keyword] = canonical.map(x => x: Parser[String]).reduce(_ | _) ^^^ this
  }
  // implicit for convenience
  protected implicit def asParser(k: Keyword): Parser[Keyword] = k.parser

  // keywords
  protected val ABS = Keyword("ABS", fun = true)
  protected val ALL = Keyword("ALL")
  protected val AND = Keyword("AND")
  protected val ARRAYS = Keyword("ARRAYS")
  protected val AS = Keyword("AS")
  protected val ASSUMING = Keyword("ASSUMING")
  protected val ASC = Keyword("ASC")
  protected val AVG = Keyword("AVG", fun = true)
  protected val AVGS = Keyword("AVGS", fun = true)
  protected val BETWEEN = Keyword("BETWEEN")
  protected val BOOLEAN = Keyword("BOOLEAN")
  protected val BY = Keyword("BY")
  protected val CASE = Keyword("CASE")
  protected val CONCATENATE = Keyword("CONCATENATE", fun = true)
  protected val COUNT = Keyword("COUNT", fun = true)
  protected val CREATE = Keyword("CREATE")
  protected val DATA = Keyword("DATA")
  protected val DATE = Keyword("DATE")
  protected val DELETE = Keyword("DELETE")
  protected val DELTAS = Keyword("DELTAS", fun = true)
  protected val DESC = Keyword("DESC")
  protected val DISTINCT = Keyword("DISTINCT", fun = true)
  protected val DROP = Keyword("DROP", fun = true)
  protected val ELSE = Keyword("ELSE")
  protected val END = Keyword("END")
  protected val EVEN = Keyword("EVEN")
  protected val EVERY = Keyword("EVERY")
  protected val EXCEPT = Keyword("EXCEPT")
  protected val EXEC = Keyword("EXEC")
  protected val EXISTS = Keyword("EXISTS")
  protected val FALSE = Keyword("FALSE")
  protected val FIELDS = Keyword("FIELDS")
  protected val FILL = Keyword("FILL", fun = true)
  protected val FILLS = Keyword("FILLS", fun = true)
  protected val FIRST = Keyword("FIRST", fun = true)
  protected val FLATTEN = Keyword("FLATTEN", fun = true)
  protected val FLOAT = Keyword("FLOAT")
  protected val FROM = Keyword("FROM")
  protected val FULL = Keyword("FULL")
  protected val FUNCTION = Keyword("FUNCTION")
  protected val GROUP = Keyword("GROUP")
  protected val HAVING = Keyword("HAVING")
  protected val IF = Keyword("IF")
  protected val IN = Keyword("IN")
  protected val INFILE = Keyword("INFILE")
  protected val INNER = Keyword("INNER")
  protected val INSERT = Keyword("INSERT")
  protected val INT = Keyword("INT")
  protected val INTO = Keyword("INTO")
  protected val IS = Keyword("IS")
  protected val JOIN = Keyword("JOIN")
  protected val LAST = Keyword("LAST", fun = true)
  protected val LIKE = Keyword("LIKE")
  protected val LOAD = Keyword("LOAD")
  protected val MAX = Keyword("MAX", fun = true)
  protected val MAXS = Keyword("MAXS", fun = true)
  protected val MIN = Keyword("MIN", fun = true)
  protected val MINS = Keyword("MINS", fun = true)
  protected val MOVING = Keyword("MOVING", fun = true)
  protected val NOT = Keyword("NOT", fun = true)
  protected val NULL = Keyword("NULL")
  protected val ODD = Keyword("ODD")
  protected val ON = Keyword("ON")
  protected val OR = Keyword("OR", fun = true)
  protected val OUTER = Keyword("OUTER")
  protected val OUTFILE = Keyword("OUTFILE")
  protected val OVERLAPS = Keyword("OVERLAPS")
  protected val PREV = Keyword("PREV", fun = true)
  protected val PRD = Keyword("PRD", fun = true)
  protected val PRDS = Keyword("PRDS", fun = true)
  protected val REVERSE = Keyword("REVERSE", fun = true)
  protected val ROWID = Keyword("ROWID")
  protected val SAVE = Keyword("SAVE")
  protected val SET =  Keyword("SET")
  protected val SELECT = Keyword("SELECT")
  protected val SHOW = Keyword("SHOW")
  protected val SQRT = Keyword("SQRT", fun = true)
  protected val STDDEV = Keyword("STDDEV", fun = true)
  protected val STRING = Keyword("STRING")
  protected val SUM = Keyword("SUM", fun = true)
  protected val SUMS = Keyword("SUMS", fun = true)
  protected val TABLE = Keyword("TABLE")
  protected val TERMINATED = Keyword("TERMINATED")
  protected val THEN = Keyword("THEN")
  protected val TIMESTAMP = Keyword("TIMESTAMP")
  protected val TRUE = Keyword("TRUE")
  protected val UPDATE = Keyword("UPDATE")
  protected val USING = Keyword("USING")
  protected val VALUES = Keyword("VALUES")
  protected val VARS = Keyword("VARS", fun = true)
  protected val WITH = Keyword("WITH")
  protected val WHEN = Keyword("WHEN")
  protected val WHERE = Keyword("WHERE")


  // Use reflection to find the reserved words defined in this class.
  protected val reservedWords = this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword])

  // Lexicalizer
  override val lexical = new AqueryLexical(reservedWords)

  // An AQuery program is a non-empty list of top-level constructs
  def program: Parser[List[TopLevel]] =
    rep1(fullQuery
        | verbatim
        | io
        | udf
        | delete
        | update
        | create
        | insert
    )

  /*
    Queries: consist of an optional list of local queries followed by a main query
     Local queries are only visible within their given set
   */
  def fullQuery: Parser[Query] =
    positioned((WITH ~> localQuery.+).? ~ topApply ^^ { case locals ~ q => Query(locals.getOrElse(Nil), q) })

  // Local query consists of a name, column names and a relational algebra result
  def localQuery: Parser[(String, List[String], RelAlg)] =
    ident ~ ("(" ~> rep1sep(ident, ",") <~ ")").? ~
      (AS ~> "(" ~> topApply <~ ")") ^^ { case n ~ cs ~ q => (n, cs.getOrElse(Nil), q)}

  // Users can apply a function to the overall result of a query. Note that the function
  // Can only take the query as an argument. Furthermore, the function must be a keyword
  // so as to avoid ambiguity. Consider the case:
  // SELECT * FROM t g(SELECT * from N). g can be viewed as a function applied to the second
  // query or as a correlation name for the table in the first query's from-clause. Making
  // this a keyword resolves this issue.
  def topApply: Parser[RelAlg] =
    positioned(
      ((SHOW ^^^ "SHOW"
        | EXEC <~ ARRAYS ^^^ "EXEC_ARRAYS"
        | DISTINCT ^^^ "DISTINCT"
        ) ~
        ("(" ~> query <~ ")")) ^^ { case f ~ q => TopApply(f, q) }
      | query
      )

  // We assemble a query with incremental relational algebra operations
  def query: Parser[RelAlg] = positioned(
    SELECT ~>
    DISTINCT.? ~
    rep1sep(projection, ",") ~
    (FROM ~> from) ~
    (ASSUMING ~> order).?  ~
    (WHERE ~> where).? ~
    (GROUP ~> BY ~> groupBy ~ (HAVING ~> rep1sep(expr, ",")).? ^^ {
      case g ~ h => (g, h.getOrElse(Nil))
    }).? ^^ {
      case d ~ p ~ t ~ s ~ w ~ gh =>
        val sorted = s.map(SortBy(t, _)).getOrElse(t)
        val filtered = w.map(Filter(sorted, _)).getOrElse(sorted)
        val grouped = gh.map { case (g, h) => GroupBy(filtered, g, h) }.getOrElse(filtered)
        val projected = Project(grouped, p)
        d.map(x => TopApply(x.str, projected)).getOrElse(projected)
    })

  // Projections
  def projection: Parser[(Expr, Option[String])] =
    expr ~ (AS ~> ident).? ^^ { case e ~ nm  => e -> nm }

  // From clause
  // for now we only consider joins based on USING. Condition based joins can use ',' and filters
  // Joins are left-associative
  lazy val from: PackratParser[RelAlg] =
    positioned(from ~ (INNER ~> JOIN ~> simpleTable) ~ (USING ~> using) ^^ { case l ~ r ~ c =>
      Join(InnerJoinUsing, l, r, c)
    }
     | from ~ (FULL ~> OUTER ~> JOIN ~> simpleTable) ~ (USING ~> using) ^^ { case l ~ r ~ c =>
        Join(FullOuterJoinUsing, l, r, c)
    }
     | (from <~ ",") ~ simpleTable ^^ { case l ~ r => Join(Cross, l, r, Nil) }
     | bottomApply
     | simpleTable
     )

  // Parentheses are optional when joining on a single column
  def using: Parser[List[Expr]] =
    ("(" ~> repsep(id, ",") <~ ")"
      | id ^^ { List(_) }
      )

  // Simple table name with optional alias ("as" is also optional)
  // So t, t as t1, and t t1 are all valid simple tables
  def simpleTable: Parser[RelAlg] =
    positioned(ident ~ (AS.? ~> ident).? ^^ {case n ~ a => Table(n, a)})

  // Generate a table from a function call
  // note that these cannot be aliased. This is to restrict their use.
  // Users can easily do something like
  // WITH myTable AS (SELECT * FROM f()) SELECT mt.c1 FROM myTable as mt
  def bottomApply: Parser[RelAlg] =
     positioned(funCall ^^ { case FunCall(f, args) => BottomApply(f, args) })

  // Order-clause, tuples of direction and column
  def order: Parser[List[(OrderDirection, String)]] = rep1sep(orderPair, ",")
  // tuples of order, note that the column can be table.columns
  def orderPair: Parser[(OrderDirection, String)] =
    (ASC| DESC) ~ ident ~ ("." ~> ident).? ^^ { case d ~ id ~ idop =>
        val col = idop.map(c => id + "." + c).getOrElse(id)
        val dir = if (d == ASC) Asc else Desc
        dir -> col
    }

  // Where clause is an "AND" separated list of predicates or expressions
  def where: Parser[List[Expr]] = rep1sep(predicate | expr, AND)


  // Predicates commonly used in where clauses
  // This is a bit dense but the general idea is we're trying to avoid repetition just due
  // to optional NOT. NOT becomes a wrapping function that simply negates original predicate
  // predicates become function calls
  def predicate: Parser[Expr] =
    positioned(expr ~ NOT.? ~ (
      (BETWEEN ~ (expr <~ AND) ~ expr) ^^ { case b ~ e1 ~ e2 => FunCall(b.str, List(e1, e2)) }
        | IN ~ (
          expr ^^ { List(_) }
          | "(" ~> rep1sep(expr, ",") <~ ")"
          ) ^^ { case i ~ es => FunCall(i.str, es) }
        | LIKE ~ expr ^^ { case l ~ e => FunCall(l.str, List(e)) }
        ) ^^ { case e ~ n ~ FunCall(f, args) =>
        wrapWithOptFun(n.map(_.str), FunCall(f, e :: args))
    }
      | expr ~ IS ~ NOT.? ~ bool ^^ { case e ~ i ~ n ~ b =>
      BinExpr(n.map( _ => Neq).getOrElse(Eq), e, b)
    }
      | expr ~ IS ~ NOT.? <~ NULL ^^ { case e ~ i ~ n =>
      wrapWithOptFun(n.map(_.str), FunCall(NULL.str, List(e)))
    }
      | range ~ NOT.? ~ OVERLAPS ~ range ^^ { case l ~ n ~ o ~ r =>
      wrapWithOptFun(n.map(_.str), FunCall(o.str, l ++ r))
    }
    )

  // simple helper for optional function call
  def wrapWithOptFun(k: Option[String], e: Expr) = k.map(x => FunCall(x, e :: Nil)).getOrElse(e)

  // range is a 2-tuple of expressions
  def range: Parser[List[Expr]] =
    ("(" ~> expr <~ ",") ~ expr <~ ")" ^^ { case e1 ~ e2 => List(e1, e2) }

  // Group by clause
  // Parse name-value pairs for groups
  def groupBy: Parser[List[(Expr, Option[String])]] =  rep1sep(group, ",")
  // Groups can be optionally named, so that they can then be reused in projection
  def group: Parser[(Expr, Option[String])] = expr ~ (AS ~> ident).? ^^ {case e ~ i => e -> i}

  // Verbatim code, handled at tokenizer level
  def verbatim: Parser[VerbatimCode] =
    positioned(elem("verbatim code", _.isInstanceOf[lexical.Verbatim]) ^^ { c => VerbatimCode(c.chars) })

  // Update statements
  // Allow order clauses (applied to entire table before update)
  // Selections
  // Group by (with optional having)
  def update: Parser[Update] =
    positioned(UPDATE ~> ident ~
      (SET ~> rep1sep(elemUpdate, ",")) ~
      (ASSUMING ~> order).? ~
      (WHERE ~> where).? ~
      (GROUP ~> BY ~> rep1sep(expr, ",") ~ (HAVING ~> rep1sep(expr, ",")).? ^^ { case g ~ h =>
        (g, h.getOrElse(Nil))
      }).? ^^ { case t ~ u ~ o ~ w ~ gh  =>
      val (gL, hL) = gh.getOrElse((Nil, Nil))
      Update(t, u, o.getOrElse(Nil), w.getOrElse(Nil), gL, hL)
    })

  // one element in an update statement
  def elemUpdate: Parser[(String, Expr)] = (ident <~ "=") ~ expr ^^ { case f ~ v => f -> v }

  // Aquery allows two type of deletes
  // deletion based on predicates (case 1) (i.e. row-wise deletion)
  // deletion based on columns (case 2) (i.e. col-wise deletion)
  // Note that group-by must have a having clause
  def delete: Parser[Delete] =
    positioned(DELETE ~> FROM ~> ident ~
      (ASSUMING ~> order).? ~
      (WHERE ~> where).? ~
      (GROUP ~> BY ~> rep1sep(expr, ",") ~ (HAVING ~> rep1sep(expr, ","))).? ^^ {
        case t ~ o ~ w ~ gh =>
          // extract lists of group/having expressions
          val (gL, hL) = gh.map { case g ~ h => (g, h) }.getOrElse((Nil, Nil))
          Delete(t, Right(w.getOrElse(Nil)), o.getOrElse(Nil), gL, hL)
      }
     | (DELETE ~> rep1sep(ident, ",") <~ FROM) ~ ident ^^ { case cs ~ t =>
      Delete(t, Left(cs), Nil, Nil, Nil)
    }
     )

  // Insert statements
  // The source of data can be explicit values or a query
  // the order of insertion can also be modified
  // the order clause applies to the new data to be inserted
  def insert: Parser[Insert] =
    positioned(INSERT ~> INTO ~> ident ~
      (ASSUMING ~> order).? ~
      modInsert.? ~
      srcInsert ^^ { case n ~ o ~ m ~ s => Insert(n, o.getOrElse(Nil), m.getOrElse(Nil), s)})

  // Insertion order can be modified by specifying the columns to put value sinto
  def modInsert: Parser[List[String]] = "(" ~> repsep(ident, ",") <~ ")"

  // Insertion can come from explicit values or a full query
  def srcInsert: Parser[Either[List[Expr], Query]] =
    (VALUES ~> "(" ~> repsep(expr, ",") <~ ")" ^^ { es => Left(es) }
      | fullQuery ^^ { Right(_) }
      )

  // Create statements
  // A table can be created simply by stating the schema, or by a query reuslt
  def create: Parser[Create] =
    positioned(CREATE ~> TABLE ~> ident ~
      (schema ^^ { Left(_) }
        | fullQuery ^^ { Right(_) }
        ) ^^ { case n ~ s => Create(n, s) })

  // A table schema consists of column name and type tuples
  def schema: Parser[List[(String, TypeName)]] = "(" ~> rep1sep(schemaEntry, ",") <~ ")"

  def schemaEntry: Parser[(String, TypeName)] = ident ~ typeLabel ^^ { case i ~ t => i -> t}

  // Types must always be in upper case, see alwaysUpperCase
  def typeLabel: Parser[TypeName] =
    positioned(INT  ^^^ TypeInt
      | FLOAT  ^^^ TypeFloat
      | TIMESTAMP ^^^ TypeTimestamp
      | DATE  ^^^ TypeDate
      | BOOLEAN ^^^ TypeBoolean
      | STRING ^^^ TypeString
      )

  // Load/Save statements
  def io: Parser[DataIO] =
  positioned( LOAD ~> DATA ~> INFILE ~> stringLit ~
    (INTO ~> TABLE ~> ident) ~
    (FIELDS ~> TERMINATED ~> BY ~> stringLit) ^^ { case f ~ t ~ s => Load(f, t, s)}
  | (fullQuery <~ INTO <~ OUTFILE) ~
    (stringLit <~ FIELDS <~ TERMINATED <~ BY) ~ stringLit ^^ { case q ~ f ~ s => Save(f, q, s) }
    )

  // User defined functions
  // UDFs are a series of semi-colon separated local variable assignments or expressions
  // The last expression/assignment corresponds to the overall function value
  def udf: Parser[UDF] =
    positioned(FUNCTION ~> ident ~
      ("(" ~> repsep(ident, ",") <~ ")") ~
      ("{" ~> repsep(udfBody, ";") <~ "}") ^^ { case n ~ a ~ b => UDF(n, a, b) })

  // UDF body: assignments to local vars or expressions
  def udfBody: Parser[Either[Assign, Expr]] =
    ((ident <~ ":=") ~ expr ^^ { case n ~ e => Left(Assign(n, e)) }
      | expr ^^ { Right(_) }
      )

  // Expressions, productions encode precedence and associativity
  lazy val expr: PackratParser[Expr] = positioned(
    (expr <~ "|") ~ and ^^ { case o ~ a => BinExpr(Lor, o, a) }
    | and
   )

  lazy val and: PackratParser[Expr] = positioned(
      (and <~ "&") ~ eq ^^ { case a ~ e => BinExpr(Land, a, e) }
      | eq
      )

  lazy val eq: PackratParser[Expr] = positioned(
    eq ~ ("=" | "!=") ~ rel ^^ { case e ~ op ~ r => BinExpr(if(op == "==") Eq else Neq, e, r) }
    |  rel
    )

  lazy val rel: PackratParser[Expr] = positioned(
    rel ~ ("<=" | ">=" | "<" | ">") ~ add ^^ {
        case r ~ opStr ~ a => BinExpr(opStr match {
          case "<=" => Le
          case ">=" => Ge
          case "<" => Lt
          case ">" => Gt
        }, r, a)
      }
    | add
    )

  lazy val add: PackratParser[Expr] = positioned(
    add ~ ("+" | "-") ~ mult ^^ {case a ~ op ~ m => BinExpr(if(op == "+") Plus else Minus, a, m) }
    | mult
   )

  lazy val mult: PackratParser[Expr] = positioned(
    mult ~ ("*" | "/") ~ pow ^^ { case m ~ op ~ p => BinExpr(if(op == "*") Times else Div, m, p) }
    | pow
    )

  def pow: Parser[Expr] = positioned(
    (unary <~ "^") ~ pow ^^ { case  u ~ e => BinExpr(Exp, u, e) }
    | unary
    )

  def unary: Parser[Expr] =
    positioned(("!" | "-") ~ baseExp ^^ { case op ~ e => if(op == "!") UnExpr(Not, e) else
      e match {
        case IntLit(v) => IntLit(-v)
        case FloatLit(v) => FloatLit(-v)
        case _ => UnExpr(Neg, e)
      }
    }
      | baseExp
      )

  // index operation
  lazy val indexExpr: PackratParser[ArrayIndex] =
    positioned(baseExp ~ ("[" ~> indexOp <~ "]") ^^ { case a ~ i => ArrayIndex(a, i) })

  // safe indexing operations
  def indexOp: Parser[IndexOperator] =
    (EVEN ^^ { _ => Even }
      | ODD ^^ {_ => Odd}
      | EVERY ~> numericLit ^^ {n => Every(n.toInt)}
      )

  // Case expressions
  def caseExpr: Parser[Case] =
    positioned(CASE ~> expr.? ~ whenThen.+ ~ (ELSE ~> expr).? <~ END ^^ { case c ~ it ~ e  => Case(c, it, e) })

  // when-then expressions, used in case expressions
  def whenThen: Parser[IfThen] =
    positioned(WHEN ~> expr <~ THEN) ~  expr ^^ { case i ~ t => IfThen(i, t) }


  // function call (handle UDF and built-in separately to avoid conflict with reserved words)
  def funCall: Parser[FunCall] =
    positioned(ident ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ { case f ~ args => FunCall(f, args) }
      | builtin ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ { case f ~ args => FunCall(f.str.toUpperCase, args) }
      )

  // reflection to get all built-in functions
  def builtin: Parser[Keyword] =
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword])
      .filter(_.fun)
      .map(_.parser).reduce(_ | _)


  lazy val baseExp: PackratParser[Expr] =
    positioned(lit
      | indexExpr
      | "(" ~> expr <~ ")"
      | caseExpr
      | funCall
      | tableExp
      | id
      )


  // Constants/literals
  def lit: Parser[Lit] =
    positioned(numericLit ^^ { v => if (v.contains(".")) FloatLit(v.toDouble) else IntLit(v.toLong) }
      | timestamp
      |  date
      |  stringLit ^^ {s => StringLit(s)}
      |  bool
      )

  // Expressions related to a table
  def tableExp: Parser[TableExpr] =
    positioned(ROWID ^^ { _ => RowId}
    | (ident <~ ".") ~ ident ^^ { case t ~ c => ColumnAccess(t, c) }
    | "*" ^^ { _ => WildCard }
    )

  def bool: Parser[Lit] =
    positioned(TRUE ^^ { _ => BooleanLit(true) }
      | FALSE ^^ { _ => BooleanLit(false) }
      )

  def date: Parser[Lit] =
    positioned(elem("date", _.isInstanceOf[lexical.Date]) ^^ { x => DateLit(x.chars) })

  def timestamp: Parser[Lit] =
    positioned(elem("timestamp", _.isInstanceOf[lexical.Timestamp]) ^^ { x => TimestampLit(x.chars) })

  def id: Parser[Id] = positioned(ident ^^ {n => Id(n)})

  /**
   * Parse a string as a given non-terminal
   * @param prod non-terminal
   * @param s string to parse
   * @tparam A
   * @return parsed representation
   */
  def parse[A](prod: Parser[A], s: String) = {
    val tokens = new lexical.Scanner(s)
    phrase(prod)(tokens)
  }

  /**
   * Parse AQuery program
   * @param s string representation of program
   * @return sequence of top-level constructs
   */
  def apply(s: String): ParseResult[Seq[TopLevel]] = parse(program, s)
}