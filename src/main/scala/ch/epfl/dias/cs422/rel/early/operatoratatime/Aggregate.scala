package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import ch.epfl.dias.cs422.rel.common.Aggregating
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  override def execute(): IndexedSeq[Column] = {
    return Aggregating.aggregate(input.toIndexedSeq.transpose, groupSet.toArray, aggCalls).transpose;
  }
}
