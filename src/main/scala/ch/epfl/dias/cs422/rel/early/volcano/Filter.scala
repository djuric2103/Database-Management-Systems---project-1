package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  lazy val current : Iterator[Tuple] = input.iterator;

  override def open(): Unit = {
  }

  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def next(): Tuple = {
    while(current.hasNext){
      val tup = current.next();
      if(e(tup) == true) {
        return tup
      };
    }
    return null;
  }

  override def close(): Unit = {
  }
}
