package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import ch.epfl.dias.cs422.rel.common.ScanOperatorAtTheTime
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}


class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  lazy val vids: IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]));
    for (i <- 0L until store.getRowCount) yield IndexedSeq(i);
  }

  override def execute(): IndexedSeq[Column] = vids

  private lazy val evals = {
    var list: List[(Long => Any)] = List();
    val tbl = ScanOperatorAtTheTime.scan(table, tableToStore);

    for (i <- 0 until tbl.size) {
      list = list :+ (vid => tbl(i)(vid.asInstanceOf[Int]));
    }
    new LazyEvaluatorAccess(list);
  }

  override def evaluators(): LazyEvaluatorAccess = evals
}
