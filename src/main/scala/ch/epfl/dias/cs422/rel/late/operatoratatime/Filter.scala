package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.{Evaluator, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val table = input.toIndexedSeq.transpose;
    var vids = IndexedSeq[Column]();
    for(i <- 0 until table.size){
      if(e(table(i)) == true){
        vids = vids :+ IndexedSeq(i);
      }
    }
    return vids;
  }

  lazy val e: Evaluator = eval(condition, input.getRowType, input.evaluators())

  override def evaluators(): LazyEvaluatorRoot = {
    input.evaluators();
  }
}
