This is a clean, scala-based, rewrite of the AQuery system.

This is a temporary repo, and eventually all contents here will be merged
with the original AQuery repo.


edu.nyu.aquery.ast contains all representations for AST
edu.nyu.aquery.parse contains all parsing code

TODO:
* analyzer (serial)
   * order-dependence
     * functions (mutual recursion)
     * expressions in queries
   * grouped
   * join references
* Optimizer (serial)
* Code gen (serial)

* Analyzer (parallel)
* Optimizer (parallel)
* Code gen (parallel)
