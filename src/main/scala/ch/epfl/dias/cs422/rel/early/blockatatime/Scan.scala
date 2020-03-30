package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Column, Elem, PAXPage, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}


class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected lazy val store: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  var curr = 0;
  var currPaxPageInd = 0;
  var currPaxInd = 0;
  var currPaxPage : PAXPage = IndexedSeq(IndexedSeq());
  val elemPerMiniPage = 4;
  lazy val numOfPages = store.getRowCount.asInstanceOf[Int] / elemPerMiniPage + (if (store.getRowCount.asInstanceOf[Int] % elemPerMiniPage == 0) 0 else 1);
  lazy val numOfFields = table.getRowType.getFieldCount();
  lazy val numberOfRows = store.getRowCount;

  override def open(): Unit = {
  }

  def getBlockRowStore(rs: RowStore): Block = {
    var block = IndexedSeq[Tuple]();
    for(i <- 0 until blockSize){
      if(curr + i >= numberOfRows) {
        curr += i;
        return block;
      }
      block = block :+ rs.getRow(curr + i);
    }
    curr += blockSize;
    return block;
  }

  def getBlockColumnStore(cs: ColumnStore): Block = {
    var block = IndexedSeq[Column]();

    for(j <- 0 until numOfFields){
      var newCol = IndexedSeq[Elem]();
      var i = 0;
      val currCol : IndexedSeq[Elem] = cs.getColumn(j);
      while(i < blockSize && curr + i < numberOfRows){
        newCol = newCol :+ currCol(curr + i);
        i += 1;
      }
      block = block :+ newCol;
    }
    curr += block(0).size;
    return block.transpose;
  }

  def getBlockPaxStore(ps: PAXStore): Block = {
    /*val page = ps.getPAXPage(currPaxPageInd).transpose;
    currPaxPageInd += 1;
    curr += page.size;
    return page;*/
    var block = IndexedSeq[Tuple]();
    while(block.size < blockSize){
      if(currPaxPageInd >= numOfPages && currPaxInd >= currPaxPage(0).size) return block;
      if(currPaxInd >= currPaxPage(0).size){
        currPaxInd = 0;
        currPaxPage = ps.getPAXPage(currPaxPageInd);
        currPaxPageInd += 1;
      }else{
        var tup = IndexedSeq[Elem]();
        for(i <- 0 until currPaxPage.size){
          tup = tup :+ currPaxPage(i)(currPaxInd)
        }
        currPaxInd += 1;
        curr += 1;
        block = block :+ tup;
      }
    }
    return block;
  }

  override def next(): Block = {
    if(curr >= numberOfRows)
      return null;
    var t = store match {
      case cs : ColumnStore => getBlockColumnStore(cs);
      case rs : RowStore => getBlockRowStore(rs);
      case ps : PAXStore => getBlockPaxStore(ps);
      case _ => null;
    }
    return t;
  }

  override def close(): Unit = {
    curr = 0;
  }
}
