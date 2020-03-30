package ch.epfl.dias.cs422.rel.common

import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}

object Joining {
  def getFields(t : Tuple, keys : IndexedSeq[Int]) : IndexedSeq[Elem] ={
    var fields = IndexedSeq[Elem]();
    for(i <- keys)
      fields = fields :+ t(i);
    return fields;
  }


  def mapp(right: IndexedSeq[Tuple], getRightKeys: IndexedSeq[Int]): mutable.HashMap[Tuple, mutable.Set[Tuple]] with mutable.MultiMap[Tuple, Tuple] = {
    val mapped = new HashMap[Tuple, Set[Tuple]] with MultiMap[Tuple, Tuple];
    for(r <- right) {
      mapped.addBinding(getFields(r, getRightKeys), r);
    }
    return mapped;
  }


  def getPairs(mapped: mutable.HashMap[Tuple, mutable.Set[Tuple]] with mutable.MultiMap[Tuple, Tuple], l: Tuple, getLeftKeys: IndexedSeq[Int]): mutable.Set[Tuple] = {
    val value = mapped.get(getFields(l, getLeftKeys));
    return value match {
      case s : Some[Set[Tuple]] => s.get;
      case None => Set[Tuple]();
    }
  }

  def join(left: IndexedSeq[Tuple], right: IndexedSeq[Tuple], getLeftKeys: IndexedSeq[Int], getRightKeys: IndexedSeq[Int]): IndexedSeq[Tuple] = {
    var joined = IndexedSeq[Tuple]();
    val mapped = mapp(right, getRightKeys);
    for(l <- left) {
      val s : Set[Tuple] = getPairs(mapped, l, getLeftKeys);
      for (r <- s) {
        val concated = l ++ r;
        joined = joined :+ concated;
      }
    }
    return joined;
  }
}
