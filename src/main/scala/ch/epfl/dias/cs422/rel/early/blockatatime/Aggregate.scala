package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import ch.epfl.dias.cs422.rel.common.Aggregating
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  var output = IndexedSeq[Tuple]();
  var curr : Iterator[Tuple] = null;


  override def open(): Unit = {
    output = Aggregating.aggregate(BlockConvert.toIndSeq(input.iterator), groupSet.toArray, aggCalls);
    curr = output.iterator;
  }

  override def next(): Block = {
    return BlockConvert.getNext(curr, blockSize);
  }

  override def close(): Unit = {
    curr = null;
    output = null;
  }
}
