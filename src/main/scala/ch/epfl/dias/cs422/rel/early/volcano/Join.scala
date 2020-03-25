package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Elem

import scala.collection.mutable

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  var joineed : IndexedSeq[Tuple] = null;
  var curr : Iterator[Tuple] = null;
  var mapped : mutable.HashMap[IndexedSeq[Elem], Tuple] = null;
  var a = "asd";

  override def open(): Unit = {
    joineed = IndexedSeq[Tuple]();
    mapped = new mutable.HashMap[IndexedSeq[Elem], Tuple];


    for(u <- right) {
      mapped.addOne(getFields(u, getRightKeys) -> u);
    }
    var counter = 0;
    for(u <- left) {
      val corr = mapped.get(getFields(u, getLeftKeys));

      for (m <- corr) {
        //println("inside")
        val r = u ++ m;
        joineed = joineed :+ r;
        counter += 1;
      }
    }
    curr = joineed.iterator;
    println("COUNTER   "+counter);
  }

  def getFields(t : Tuple, keys : IndexedSeq[Int]) : IndexedSeq[Elem] ={
    var fields = IndexedSeq[Elem]();
    for(i <- keys)
      fields = fields :+ t(i);
    return fields;
  }

  override def next(): Tuple = {
    if(curr.hasNext)
      return curr.next();
    return null;
  }

  override def close(): Unit = {

  }
}
