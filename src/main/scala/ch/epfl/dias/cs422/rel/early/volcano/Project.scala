package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._


class Project protected (input: Operator, projects: java.util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  lazy val current = input.iterator;
  //val n = current.size;
  //var i = 0;
  override def open(): Unit = {
  }

  lazy val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)

  override def next(): Tuple = {
    if(current.hasNext) {
      val tup = current.next();
      return evaluator(tup);
    }
    return null;
  }

  override def close(): Unit = {
    //current = null;
//    input.close();

  }
}
