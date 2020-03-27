package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Column, Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}


class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected lazy val store: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  var curr = 0;
  var currPaxPage = 0;

  override def open(): Unit = {
  }

  def getRowRowStore(rs: RowStore): Block = {
    var block = IndexedSeq[Tuple]();
    for(i <- 0 until blockSize){
      if(curr + i >= rs.getRowCount) {
        curr += i;
        return block;
      }
      block = block :+ rs.getRow(curr + i);
    }
    curr += blockSize;
    return block;
  }

  def getRowCoulumnStore(cs: ColumnStore): Block = {
    var block = IndexedSeq[Column]();
    val n = table.getRowType.getFieldCount()

    for(j <- 0 until n){
      var newCol = IndexedSeq[Elem]();
      var i = 0;
      val currCol : IndexedSeq[Elem] = cs.getColumn(j);
      while(i < blockSize && curr + i < cs.getRowCount){
        newCol = newCol :+ currCol(curr + i);
        i += 1;
      }
      block = block :+ (newCol.asInstanceOf[Column]);
    }
    curr += block(0).size;
    return block.transpose;
  }

  def getRowPaxStore(ps: PAXStore): Block = {
    val page = ps.getPAXPage(currPaxPage).transpose;
    currPaxPage += 1;
    curr += page.size;
    return page;
  }

  override def next(): Block = {
    if(curr >= store.getRowCount)
      return null;
    var t = store match {
      case cs : ColumnStore => getRowCoulumnStore(cs);
      case rs : RowStore => getRowRowStore(rs);
      case ps : PAXStore => getRowPaxStore(ps);
      case _ => null;
    }
    return t;
  }

  override def close(): Unit = {
    curr = 0;
  }
}
