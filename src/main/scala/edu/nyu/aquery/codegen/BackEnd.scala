package edu.nyu.aquery.codegen

import edu.nyu.aquery.ast.TopLevel

/**
 * General trait for back-end code generators
 */
trait BackEnd {
  def generate(prog: Seq[TopLevel]): String
}

