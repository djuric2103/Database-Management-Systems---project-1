package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val table = input.toIndexedSeq.transpose;

    var output = IndexedSeq[Tuple]();

    val e: Tuple => Any = eval(condition, input.getRowType)

    for (i <- 0 until table.size) {
      if (e(table(i)) == true) {
        output = output :+ table(i);
      }
    }

    return output.transpose;
  }
}
