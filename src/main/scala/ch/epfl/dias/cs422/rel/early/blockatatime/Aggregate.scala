package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import ch.epfl.dias.cs422.rel.common.Aggregating
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable.{HashMap, MultiMap, Set}

class Aggregate protected(input: Operator,
                          groupSet: ImmutableBitSet,
                          aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  var table: IndexedSeq[Tuple] = null;
  var key_fields: Array[Int] = null;
  var reduction = false;
  var reductionFirstCall = true;
  var mapa: HashMap[Tuple, Set[Int]] with MultiMap[Tuple, Int] = null;
  var keys: IndexedSeq[Tuple] = null;
  var currentKey = 0;

  override def open(): Unit = {
    table = BlockConvert.toIndSeq(input.iterator);
    key_fields = groupSet.toArray;
    if (key_fields.size == 0) {
      reduction = true;
      reductionFirstCall = true;
    } else {
      mapa = Aggregating.grouping(table, key_fields);
      keys = mapa.keySet.toIndexedSeq;
      currentKey = 0;
    }
  }


  def nextTuple(): Tuple = {
    if (reduction) {
      if (reductionFirstCall) {
        reductionFirstCall = false;
        return Aggregating.reduction(table, aggCalls);
      }
      return null;
    }
    if (currentKey >= keys.size) return null;
    currentKey += 1;
    val group = mapa.get(keys(currentKey - 1));
    return keys(currentKey - 1) ++ Aggregating.reduction(Aggregating.getSubtable(table, group), aggCalls);
  }

  override def next(): Block = {
    var block = IndexedSeq[Tuple]();
    while (block.size < blockSize) {
      val tup = nextTuple();
      if (tup == null) {
        if (block.size == 0) return null;
        return block;
      }
      block = block :+ tup;
    }
    return block;
  }

  override def close(): Unit = {
    var table = null;
    var key_fields = null;
    var reduction = false;
    var reductionFirstCall = true;
    var mapa = null;
    var keys = null;
    var currentKey = 0;
  }

}
