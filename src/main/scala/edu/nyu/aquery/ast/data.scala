package edu.nyu.aquery.ast

/**
 * Table modifications refer to actions that modify a table. This includes
 * table creation, and insertion of values
 *
 * This constructs are not optimized so we have a trivial transform
 */
trait TableModification extends AST[TableModification] with TopLevel {
  def transform(f: PartialFunction[TableModification, TableModification]) = this
}

/**
 * Create a table
 * @param n name of table
 * @param source either a scheme (which declares the table) or a query, where the results
 *               create the table
 */
case class Create(
  n: String,
  source: Either[List[(String, TypeName)], Query]) extends TableModification {
  def dotify(currAvail: Int) = {
    val selfNode = Dot.declareNode(currAvail, "create")
    val nameEdge = Dot.declareEdge(currAvail, currAvail + 1)
    val nameNode = Dot.declareNode(currAvail + 1, n)
    val srcId = currAvail + 2
    val srcEdge = Dot.declareEdge(currAvail, srcId)
    val (srcNode, nextAvail) = source match {
      case Left(xs) =>
        val label = xs.map(x => x._1 + ":" + x._2).mkString(",")
        (Dot.declareNode(srcId, label), srcId + 1)
      case Right(q) => q.dotify(srcId)
    }
    (selfNode + nameEdge + nameNode + srcNode, nextAvail)
  }
}

/**
 * Insert statement
 * @param n name of table to insert into
 * @param order assumed order of data prior to insertion
 * @param modifier possible list of columns for insertion order
 * @param src source of data, either a list of values or a query
 */
case class Insert(
  n: String,
  order: List[(OrderDirection, Expr)],
  modifier: List[String],
  src: Either[List[Expr], Query]) extends TableModification {
  def dotify(currAvail: Int) = {
    val selfNode = Dot.declareNode(currAvail, "insert")
    val nameNode = Dot.declareNode(currAvail + 1, n)
    val nameEdge = Dot.declareEdge(currAvail, currAvail + 1)
    val modifierNode = Dot.declareNode(currAvail + 2, modifier.mkString(","))
    val modifierEdge = Dot.declareEdge(currAvail, currAvail + 2)
    val sourceEdge = Dot.declareEdge(currAvail, currAvail + 3)
    val (sourceNode, nextAvail) = src match {
      case Left(ex) => {
        val nodeLabel = "(" + ex.map(_.dotify(currAvail + 3)._1).mkString(",") + ")"
        val node = Dot.declareNode(currAvail + 3, nodeLabel)
        (node, currAvail + 4)
      }
      case Right(q) => q.dotify(currAvail + 3)
    }
    (selfNode + nameNode + nameEdge + modifierNode + modifierEdge + sourceEdge + sourceNode, nextAvail)
  }
}

/**
 * Encodes saving/loading data directly in AQuery from/to text files separated by a common char seq.
 * These operations are not
 */
trait DataIO extends AST[DataIO] with TopLevel {
  def file: String
  def sep: String
  def dotify(currAvail: Int) = DataIO.dotify(this, currAvail)
  def transform(f: PartialFunction[DataIO, DataIO]) = this
}

/**
 * Load data from a `sep`-separated file
 * @param file name of file
 * @param t table to load data into (must be declared)
 * @param sep string used to separate values on each line
 */
case class Load(
  file: String,
  t: String,
  sep: String) extends DataIO

/**
 * Save query to `sep`-separated file
 * @param file name of file
 * @param query table to save data from
 * @param sep string used to separate values on each line
 */
case class Save(
  file: String,
  query: Query,
  sep: String) extends DataIO

object DataIO {
  def dotify(o: DataIO, currAvail: Int) = {
    val nodeLabel = o match {
      case Save(f, _, s) => s"save query to $f with $s sep"
      case Load(f, t, s) => s"load $f into $t with $s sep"
    }
    val node = Dot.declareNode(currAvail, nodeLabel)
    (node, currAvail + 1)
  }
}