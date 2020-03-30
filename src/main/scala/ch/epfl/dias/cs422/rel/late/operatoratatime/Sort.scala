package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.rel.common.Sorting
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  def getInt(x : RexNode): Int ={
    evalLiteral(x) match {
      case i: Int => i
      case _ => 0
    }
  }

  lazy val vids = {
    val n = input.size;
    val of = if (offset != null) getInt(offset) else 0;
    val fet = if (fetch != null) getInt(fetch) else n;
    for(i <- 0L until Math.min(n - of, fet).asInstanceOf[Long]) yield  IndexedSeq(i);
  }

  override def execute(): IndexedSeq[Column] = {
    return vids;
  }

  private lazy val evals = {
    var table = input.execute().map(vid => input.evaluators()(vid))

    val of = if (offset != null) getInt(offset) else 0;
    val fet = if (fetch != null) getInt(fetch) else table.size;

    table = Sorting.sort(table,collation, of,fet);
    var list : List[(Long => Any)] = List();
    val n = if(table.size > 0) table(0).size else 0;
    for(i <- 0 until n){
      list = list :+ (vid => table(vid.asInstanceOf[Int])(i));
    }

    new LazyEvaluatorAccess(list);
  }

  override def evaluators(): LazyEvaluatorAccess = evals
}
