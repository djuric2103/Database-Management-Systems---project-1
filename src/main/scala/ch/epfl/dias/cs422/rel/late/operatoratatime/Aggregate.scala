package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{LazyEvaluatorAccess, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import ch.epfl.dias.cs422.rel.common.{Aggregating, ScanOperatorAtTheTime}
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, MultiMap, Set}

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  lazy val table = input.execute().map(vid => input.evaluators()(vid));
  lazy val key_fields = groupSet.toArray
  lazy val reduction = (key_fields.size == 0);
  lazy val mapa = Aggregating.grouping(table, key_fields);
  lazy val keys = mapa.keySet;

  lazy val vids : IndexedSeq[Column] = {
    if(reduction){
      IndexedSeq(IndexedSeq(0L));
    }else{
      for(i <- 0L until keys.size.asInstanceOf[Long]) yield IndexedSeq(i);
    }
  }

  override def execute(): IndexedSeq[Column] = vids

  private lazy val evals = {
    var output = IndexedSeq[Tuple]();
    if(reduction){
      output = output :+ Aggregating.reduction(table, aggCalls);
    }else{
      for (k <-keys) {
        val group = mapa.get(k);
        output = output :+ (k ++ Aggregating.reduction(Aggregating.getSubtable(table, group), aggCalls));
      }
    }
    var list : List[(Long => Any)] = List();
    val n = if(output.size > 0) output(0).size else 0;
    for(i <- 0 until n){
      list = list :+ (vid => output(vid.asInstanceOf[Int])(i));
    }
    new LazyEvaluatorAccess(list.toList);
  }
  override def evaluators(): LazyEvaluatorAccess = evals
}
