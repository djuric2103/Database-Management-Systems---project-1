package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected val scannable: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  var curr = 0;
  var currPaxPage = 0;
  var currPaxInd = 0;


  override def open(): Unit = {

  }



  def getRowRowStore(rs: RowStore): Tuple = {
    rs.getRow(curr)
  }

  def getRowCoulmnStore(cs: ColumnStore): Tuple = {
    var tuple = IndexedSeq[Elem]();
    val n = table.getRowType.getFieldCount()

    for(i <- 0 until n) {
      tuple = tuple :+ cs.getColumn(i)(curr);
    };

    return tuple;
  }

  def getRowPaxStore(ps: PAXStore): Tuple = {
    if(currPaxInd >= ps.getPAXPage(currPaxPage)(0).size){
      currPaxPage += 1;
      currPaxInd = 0;
    }

    var tuple = IndexedSeq[Elem]();
    val page = ps.getPAXPage(currPaxPage);
    for(i <- 0 until page.size){
      tuple = tuple :+ page(i)(currPaxInd);
    }
    currPaxInd += 1;
    return tuple;
  }

  override def next(): Tuple = {
    if(curr >= scannable.getRowCount)
      return null;
    val t = scannable match {
      case cs : ColumnStore => getRowCoulmnStore(cs);
      case rs : RowStore => getRowRowStore(rs);
      case ps : PAXStore => getRowPaxStore(ps);
      case _ => null;
    }
    curr += 1;
    return t;
  }

  override def close(): Unit = {
    curr = 0;
  }
}
