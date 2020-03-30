package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.rel.common.Sorting
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  def getInt(x : RexNode): Int ={
    evalLiteral(x) match {
      case i: Int => i
      case _ => 0
    }
  }

  lazy val executed = input.execute();
  lazy val n = executed.size;
  lazy val of = if (offset != null) getInt(offset) else 0;
  lazy val fet = if (fetch != null) getInt(fetch) else n;
  lazy val table = executed.map(vid => input.evaluators()(vid));
  lazy val vids = {
    for(i <- 0L until Math.min(n - of, fet).asInstanceOf[Long]) yield  IndexedSeq(i);
  }

  override def execute(): IndexedSeq[Column] = vids

  private lazy val evals = {
    val output = Sorting.sort(table,collation, of,fet);
    var list : List[(Long => Any)] = List();
    val n = if(output.size > 0) output(0).size else 0;
    for(i <- 0 until n){
      list = list :+ (vid => output(vid.asInstanceOf[Int])(i));
    }

    new LazyEvaluatorAccess(list);
  }

  override def evaluators(): LazyEvaluatorAccess = evals
}
