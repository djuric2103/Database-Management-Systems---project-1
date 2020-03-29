package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorRoot
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.rel.common.Joining
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.{HashMap, MultiMap, Set}

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    var joined = IndexedSeq[Column]();

    val mapped = new HashMap[Tuple, Set[Column]] with MultiMap[Tuple, Column];

    val leftLazyEvaluator = left.evaluators();
    val rightLazyEvaluator = right.evaluators();

    for(vid <- left.toIndexedSeq) {
      mapped.addBinding(Joining.getFields(leftLazyEvaluator.apply(vid), getLeftKeys), vid);
    };

    for(rightVid <- right.toIndexedSeq){
      val value = mapped.get(Joining.getFields(rightLazyEvaluator.apply(rightVid), getRightKeys));
      value match {
        case s: Some[Set[Column]] => {
          for(leftVid <- s.get){
            joined = joined :+ IndexedSeq(leftVid, rightVid);
          }
        }
      }
    }
    return joined;
  }

  private lazy val evals = lazyEval(left.evaluators(), right.evaluators(), left.getRowType, right.getRowType)

  override def evaluators(): LazyEvaluatorRoot = evals
}