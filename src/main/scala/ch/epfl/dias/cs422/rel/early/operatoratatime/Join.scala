package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.rel.common.Joining
import org.apache.calcite.rex.RexNode

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    return Joining.join(left.toIndexedSeq.transpose, right.toIndexedSeq.transpose, getLeftKeys, getRightKeys).transpose;
  }
}
