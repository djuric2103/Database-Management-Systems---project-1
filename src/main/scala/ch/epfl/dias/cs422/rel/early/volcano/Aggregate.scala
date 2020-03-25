package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet



class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  lazy val current = input.iterator;
  var table = IndexedSeq[Tuple]();
  var output = IndexedSeq[Tuple]();
  var i = 0;
  var red = false;

  def reduction(subTable : IndexedSeq[Tuple]) = {
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
    output = output :+ out_red;
  }

  def grouping(key_fields: Array[Int]) = {
    for(i <- 0 until table.size){
      var key : IndexedSeq[Elem] = null;
      key = key_fields.map(k => table(i)(k));
      //println("kljuc ")//+key.toString());
    }
  }

  override def open(): Unit = {
    while(current.hasNext){
      val tup : Tuple = current.next();
      table = table :+ tup;
    }

    val key_fields = groupSet.toArray;

    if(key_fields.size > 0){
      grouping(key_fields);
    }else {
      red = true;
      reduction(table);
    }
  }


  override def next(): Tuple = {
    if(red && i == 0){
      i += 1;
      return output(0);
    }
    return null;
  }


  override def close(): Unit = {
  }
}
