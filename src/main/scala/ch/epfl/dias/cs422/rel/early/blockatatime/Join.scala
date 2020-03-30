package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.rel.common.Joining
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.{HashMap, MultiMap, Set}

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  var tabLeft: IndexedSeq[Tuple] = null;
  var mapped: HashMap[Tuple, Set[Tuple]] with MultiMap[Tuple, Tuple] = null;
  var pairs: IndexedSeq[Tuple] = null;
  var currL = 0;
  var currPairs = 0;

  override def open(): Unit = {
    tabLeft = BlockConvert.toIndSeq(left.iterator);
    mapped = Joining.mapp(BlockConvert.toIndSeq(right.iterator), getRightKeys);
    pairs = IndexedSeq[Tuple]();
    currL = 0;
    currPairs = 0;
  }

  override def next(): Block = {
    var block = IndexedSeq[Tuple]();
    while (block.size < blockSize) {
      if (currL >= tabLeft.size && currPairs >= pairs.size) {
        if (block.size == 0) return null;
        return block;
      }
      if (currPairs >= pairs.size) {
        pairs = Joining.getPairs(mapped, tabLeft(currL), getLeftKeys).toIndexedSeq;
        currL += 1;
        currPairs = 0;
      } else {
        val tup: Tuple = tabLeft(currL - 1) ++ pairs(currPairs);
        block = block :+ tup;
        currPairs += 1;
      }
    }
    return block;
  }

  override def close(): Unit = {
    tabLeft = null;
    mapped = null;
    pairs = IndexedSeq[Tuple]();
    currL = 0;
    currPairs = 0;
  }
}
