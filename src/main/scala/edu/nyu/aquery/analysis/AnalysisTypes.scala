package edu.nyu.aquery.analysis

object AnalysisTypes {
  sealed abstract class TypeTag {
    /**
     * Checks if a type is consistent with a set of type tags. A tag is always consistent
     * if it is unknown or the set contains unknown.
     * @param s
     * @return
     */
    def consistent(s: Set[TypeTag]) = this == TUnknown || s.contains(this) || s.contains(TUnknown)
  }
  // used for int/float/date/timestamp
  case object TNumeric extends TypeTag {
    override def toString = "numeric"
  }
  case object TBoolean extends TypeTag {
    override def toString = "boolean"
  }
  case object TString extends TypeTag {
    override def toString = "string"
  }
  // represents a missing expression
  case object TUnit extends TypeTag {
    override def toString = "unit"
  }
  //case object TUDF extends TypeTag
  case object TTable extends TypeTag {
    override def toString = "table"
  }
  // unknown types at runtime
  case object TUnknown extends TypeTag {
    override def toString = "unknown"
  }

  // some convenience vals
  val bool: Set[TypeTag] = Set(TBoolean)
  val numAndBool: Set[TypeTag] = Set(TNumeric, TBoolean)
  val num: Set[TypeTag] = Set(TNumeric)
  val unk: Set[TypeTag] = Set(TUnknown)
}
