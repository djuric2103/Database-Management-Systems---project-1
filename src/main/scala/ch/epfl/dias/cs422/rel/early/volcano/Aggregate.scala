package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  val table = IndexedSeq[Tuple]();
  var red = false;
  var out_red : IndexedSeq[Elem] = null;
  var i = 0;

  def reduction() = {
    out_red = IndexedSeq[Elem](aggCalls.length);

    for(i <- 0 until aggCalls.size){
      val base = aggEmptyValue(aggCalls(i));
      out_red.updated(i, aggEmptyValue(aggCalls(i)));
      println("base "+ base);
      for(j <- 0 until table.size){
        //if(base != null)
          //out_red.updated(i, aggCalls(i).getArgument(table(j)).asInstanceOf[Int] + out_red(i).asInstanceOf[Int] );
        //else
        out_red.updated(i, aggCalls(i).reduce(out_red(i), aggCalls(i).getArgument(table(j))));
        println("\t"+aggCalls(i).getArgument(table(j)));
      }
    }
  }

  def grouping(key_fields: Array[Int]) = {
    for(i <- 0 until table.size){
      var key : IndexedSeq[Elem] = null;
      key = key_fields.map(k => table(i)(k));
      println("kljuc ")//+key.toString());
    }
  }

  override def open(): Unit = {
    for(i <- 0 until table.size){
      table +: input.next();
    }

    val key_fields = groupSet.toArray;

    if(key_fields.size > 0){
      grouping(key_fields);
    }else{
      red = true;
      reduction();
    }



  }


  override def next(): Tuple = {
    if(red && i == 0){
      i += 1;
      return out_red;
    }



    return null;
  }


  override def close(): Unit = {

  }


}
