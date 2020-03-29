package ch.epfl.dias.cs422.rel.common

import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}

import scala.collection.mutable.{HashMap, MultiMap, Set}

object Joining {
  def getFields(t : Tuple, keys : IndexedSeq[Int]) : IndexedSeq[Elem] ={
    var fields = IndexedSeq[Elem]();
    for(i <- keys)
      fields = fields :+ t(i);
    return fields;
  }


  def join(left: IndexedSeq[Tuple], right: IndexedSeq[Tuple], getLeftKeys: IndexedSeq[Int], getRightKeys: IndexedSeq[Int]): IndexedSeq[Tuple] = {
    var joined = IndexedSeq[Tuple]();

    val mapped = new HashMap[Tuple, Set[Tuple]] with MultiMap[Tuple, Tuple];

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
    return joined;
  }
}
