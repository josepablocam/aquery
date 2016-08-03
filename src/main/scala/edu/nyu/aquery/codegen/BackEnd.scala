package edu.nyu.aquery.codegen

import edu.nyu.aquery.ast.TopLevel

/**
 * General trait for back-end code generators
 */
trait BackEnd {
  /**
   * If true, backend generates calls to execute queries immediately after definition. Note
   * that update/delete/insert statements etc always run, regardless of this value
   * @return
   */
  def runQueries: Boolean
  /**
   * Generate backend code.
   * @param prog input program
   * @return
   */
  def generate(prog: Seq[TopLevel]): String
}

