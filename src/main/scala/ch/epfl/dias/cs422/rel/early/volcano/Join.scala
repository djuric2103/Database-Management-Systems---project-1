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

  var joined = IndexedSeq[Tuple]();
  var curr = 0;
  var mapped = new mutable.HashMap[IndexedSeq[Elem], Tuple];

  override def open(): Unit = {
    for(u <- right) {
      mapped.addOne(getFields(u, getRightKeys) -> u);
    }

    for(u <- left) {
      val corr = mapped.get(getFields(u, getLeftKeys));

      //println("\t\t"+corr.size);
      for (m <- corr) {
        println("inside")
        val r = u ++ m;
        joined = joined :+ r;
      }
    }
    println("joined size: "+joined.size);
  }

  def getFields(t : Tuple, keys : IndexedSeq[Int]) : IndexedSeq[Elem] ={
    var fields = IndexedSeq[Elem]();
    for(i <- keys)
      fields = fields :+ t(i);
    return fields;
  }

  override def next(): Tuple = {
    println(curr + "\tjoined size" + joined.size);
    if(curr >= joined.size)
      return null;
    curr += 1;
    return joined(curr - 1);
  }

  override def close(): Unit = {
    curr = 0;
    //joined = null;
  }
}
