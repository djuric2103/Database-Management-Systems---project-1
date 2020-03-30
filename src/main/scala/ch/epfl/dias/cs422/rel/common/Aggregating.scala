package ch.epfl.dias.cs422.rel.common

import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}

object Aggregating {
  def reduction(subTable: IndexedSeq[Tuple], aggCalls: List[AggregateCall]): Tuple = {
    var out_red = IndexedSeq[Elem]();

    for (i <- 0 until aggCalls.size) {
      var current = aggCalls(i).emptyValue;
      for (j <- 0 until subTable.size) {
        if (current == null)
          current = aggCalls(i).getArgument(subTable(j));
        else
          current = aggCalls(i).reduce(current, aggCalls(i).getArgument(subTable(j)));
      }
      out_red = out_red :+ current;
    }
    return  out_red;
  }

  def getKey(key_fields: Array[Int], tuple: Tuple): Tuple = {
    var key = IndexedSeq[Elem]();
    for (i <- 0 until key_fields.size) {
      key = key :+ tuple(key_fields(i));
    }
    return key;
  }


  def getSubtable(table: IndexedSeq[Tuple], value: Option[mutable.Set[Int]]): IndexedSeq[Tuple] = {
    var subTable = IndexedSeq[Tuple]();

    val s: Set[Int] = value match {
      case s: Some[Set[Int]] => s.get;
      case None => Set[Int]();
    }

    for (i <- s) {
      subTable = subTable :+ table(i);
    }
    return subTable;
  }

  def grouping(table: IndexedSeq[Tuple], key_fields: Array[Int]): HashMap[Tuple, Set[Int]] with MultiMap[Tuple, Int] = {
    val mapa = new HashMap[Tuple, Set[Int]] with MultiMap[Tuple, Int];

    for (i <- 0 until table.size) {
      val key = getKey(key_fields, table(i));
      mapa.addBinding(key, i);
    }
    return mapa;
  }

  def aggregate(table: IndexedSeq[Tuple], key_fields: Array[Int], aggCalls: List[AggregateCall]): IndexedSeq[Tuple] = {
    if (key_fields.size == 0)
      return IndexedSeq(reduction(table, aggCalls));

    var output = IndexedSeq[Tuple]();
    val mapa = grouping(table, key_fields);
    for (k <- mapa.keySet) {
      val group = mapa.get(k);
      output = output :+ (k ++ reduction(getSubtable(table, group), aggCalls));
    }

    return output;
  }
}
