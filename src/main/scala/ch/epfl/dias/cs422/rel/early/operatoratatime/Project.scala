package ch.epfl.dias.cs422.rel.early.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val table : IndexedSeq[Tuple]= input.toIndexedSeq.transpose;
    val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType);
    var output = IndexedSeq[Tuple]();

    for(i <- 0 until table.size){
      output = output :+ evaluator(table(i));
    }

    return output.transpose;
  }
}
