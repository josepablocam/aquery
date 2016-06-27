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
