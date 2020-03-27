package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.rel.common.Joining
import org.apache.calcite.rex.RexNode

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  var joined : IndexedSeq[Tuple] = null;
  var curr : Iterator[Tuple] = null;

  override def open(): Unit = {
    joined = Joining.join(BlockConvert.toIndSeq(left.iterator), BlockConvert.toIndSeq(right.iterator), getLeftKeys, getRightKeys);

    curr = joined.iterator;
  }

  override def next(): Block = {
    return BlockConvert.getNext(curr, blockSize);
  }

  override def close(): Unit = {
    curr = null;
    joined = null;
  }
}
