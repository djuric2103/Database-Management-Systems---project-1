package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Elem
import collection.mutable.{ HashMap, MultiMap, Set }

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  var joined = IndexedSeq[Tuple]();
  var curr : Iterator[Tuple] = null;

  val mapped = new HashMap[Tuple, Set[Tuple]] with MultiMap[Tuple, Tuple];


  override def open(): Unit = {
    for(r <- right) {
      mapped.addBinding(getFields(r, getRightKeys), r);
    }

    for(l <- left) {
      val value = mapped.get(getFields(l, getLeftKeys));
      val s : Set[Tuple] = value match {
        case s : Some[Set[Tuple]] => s.get;
        case None => Set[Tuple]();
      }
      for (r <- s) {
        val concated = l ++ r;
        joined = joined :+ concated;
      }
    }
    curr = joined.iterator;
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
    curr = null;
    joined = null;
  }
}
