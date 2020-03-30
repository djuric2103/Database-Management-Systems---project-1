package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
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
    tabLeft = left.iterator.toIndexedSeq;
    mapped = Joining.mapp(right.iterator.toIndexedSeq, getRightKeys);
    pairs = IndexedSeq[Tuple]();
    currL = 0;
    currPairs = 0;
  }

  override def next(): Tuple = {
    while (currL < tabLeft.size || currPairs < pairs.size) {
      if (currPairs >= pairs.size) {
        pairs = Joining.getPairs(mapped, tabLeft(currL), getLeftKeys).toIndexedSeq;
        currL += 1;
        currPairs = 0;
      } else {
        currPairs += 1;
        return tabLeft(currL - 1) ++ pairs(currPairs - 1);
      }
    }
    return null;
  }

  override def close(): Unit = {
    tabLeft = null;
    mapped = null;
    pairs = IndexedSeq[Tuple]();
    currL = 0;
    currPairs = 0;
  }
}
