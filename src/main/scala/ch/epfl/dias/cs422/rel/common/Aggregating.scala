package ch.epfl.dias.cs422.rel.common

import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}

object Aggregating {
  var table : IndexedSeq[Tuple] = null;
  var output : IndexedSeq[Tuple] = null;
  var curr : Iterator[Tuple] = null;
  var aggCalls: List[AggregateCall] = null;

  def reduction(key : IndexedSeq[Elem], subTable : IndexedSeq[Tuple]) = {
    var out_red = IndexedSeq[Elem]();

    for(i <- 0 until aggCalls.size){
      var current = aggCalls(i).emptyValue;
      for(j <- 0 until subTable.size){
        if(current == null)
          current = aggCalls(i).getArgument(subTable(j));
        else
          current = aggCalls(i).reduce(current, aggCalls(i).getArgument(subTable(j)));
      }
      out_red = out_red :+ current;
    }
    val line : Tuple = key ++ out_red;
    output = output :+ line;
  }

  def getKey(key_fields: Array[Int], tuple: Tuple): Tuple = {
    var key = IndexedSeq[Elem]();
    for (i <- 0 until key_fields.size) {
      key = key :+ tuple(key_fields(i));
    }
    return key;
  }


  def getSubtable(value: Option[mutable.Set[Int]]): IndexedSeq[Tuple] = {
    var subTable = IndexedSeq[Tuple]();

    val s : Set[Int] = value match {
      case s : Some[Set[Int]] => s.get;
      case None => Set[Int]();
    }

    for(i <- s){
      subTable = subTable :+ table(i);
    }
    return subTable;
  }

  def grouping(key_fields: Array[Int]) = {
    val mapa = new HashMap[Tuple, Set[Int]] with MultiMap[Tuple, Int];

    for(i <- 0 until table.size){
      val key = getKey(key_fields, table(i));
      mapa.addBinding(key, i);
    }

    for(k <- mapa.keySet){
      val value = mapa.get(k);
      reduction(k, getSubtable(value));
    }
  }

  def aggregate(tbl: IndexedSeq[Tuple], key_fields: Array[Int], aCall: List[AggregateCall]): IndexedSeq[Tuple] = {
    table = tbl;
    aggCalls = aCall;
    output = IndexedSeq[Tuple]();
    if(key_fields.size > 0)
      grouping(key_fields);
    else
      reduction(IndexedSeq[Elem](),table);
    return output;
  }
}
