package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}



class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  var table : IndexedSeq[Tuple] = null;
  var output = IndexedSeq[Tuple]();
  var curr : Iterator[Tuple] = null;


  def reduction(key : IndexedSeq[Elem], subTable : IndexedSeq[Tuple]) = {
    var out_red = IndexedSeq[Elem]();

    for(i <- 0 until aggCalls.size){
      var current = aggEmptyValue(aggCalls(i));
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

  override def open(): Unit = {
    table = input.toIndexedSeq;

    val key_fields = groupSet.toArray;

    if(key_fields.size > 0)
      grouping(key_fields);
    else
      reduction(IndexedSeq[Elem](),table);
    curr = output.iterator;
  }


  override def next(): Tuple = {
    if(curr.hasNext)
      return curr.next()
    return null;
  }


  override def close(): Unit = {
    curr = null;
    output = null;
    table = null;
  }
}
