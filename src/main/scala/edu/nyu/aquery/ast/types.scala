package edu.nyu.aquery.ast

/**
 * Describes the possible data types in AQuery. Mainly used to declare new table schemas
 */
trait TypeName extends AST[TypeName] {
  def dotify(currAvail: Int) = TypeName.dotify(this, currAvail)
  def transform(f: PartialFunction[TypeName, TypeName]) = this
}
case object TypeInt extends TypeName
case object TypeFloat extends TypeName
case object TypeString extends TypeName
case object TypeDate extends TypeName
case object TypeTimestamp extends TypeName
case object TypeBoolean extends TypeName

object TypeName {
  def dotify(o: TypeName, currAvail: Int) = {
    val typeLabel = o match {
      case TypeInt => "int"
      case TypeFloat => "float"
      case TypeString => "string"
      case TypeDate => "date"
      case TypeBoolean => "boolean"
      case TypeTimestamp => "timestamp"
    }
    val typeNode = Dot.declareNode(currAvail, typeLabel)
    (typeNode, currAvail + 1)
  }

}
