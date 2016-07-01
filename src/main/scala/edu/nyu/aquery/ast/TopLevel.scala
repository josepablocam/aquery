package edu.nyu.aquery.ast

/**
 * Any construct allowed at the top-level of an AQuery program extends the TopLevel trait
 */
trait TopLevel {
  def transform(f: PartialFunction[TopLevel, TopLevel]): TopLevel = {
    f.lift(this) match {
      case Some(t) => t
      case None => this
    }
  }
}

object TopLevel {
  def dotify(o: TopLevel, i: Int): (String, Int) = o match {
    case q: Query => q.dotify(i)
    case v: VerbatimCode => v.dotify(i)
    case io: DataIO => io.dotify(i)
    case u: UDF => u.dotify(i)
    case m: ModificationQuery => m.dotify(i)
    case t: TableModification => t.dotify(i)
  }
}
