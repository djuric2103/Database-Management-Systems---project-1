package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.rel.common.Sorting
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  def getInt(x : RexNode): Int ={
    evalLiteral(x) match {
      case i: Int => i
      case _ => 0
    }
  }

  override def execute(): IndexedSeq[Column] = {
    var table = input.toIndexedSeq.transpose;

    val of = if (offset != null) getInt(offset) else 0;
    val fet = if (fetch != null) getInt(fetch) else table.size;


    table = Sorting.sort(table, collation, of, fet);

    return table.transpose;
  }
}
