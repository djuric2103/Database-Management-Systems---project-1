package ch.epfl.dias.cs422.rel.common

import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, PAXPage, Tuple}
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.RelOptTable

object ScanOperatorAtTheTime {
  var table: RelOptTable = null;

  def getTableColumnStore(cs: ColumnStore): IndexedSeq[Column] = {
    var tbl = IndexedSeq[Column]();
    if (cs.getRowCount == 0) return tbl;


    for (i <- 0 until table.getRowType.getFieldCount()) {
      tbl = tbl :+ cs.getColumn(i);
    }

    return tbl;
  }

  def getTableRowStore(rs: RowStore): IndexedSeq[Column] = {
    var tbl = IndexedSeq[Tuple]();
    for (i <- 0 until rs.getRowCount.asInstanceOf[Int]) {
      tbl = tbl :+ rs.getRow(i);
    }
    return tbl.transpose;
  }


  def getTablePaxStore(ps: PAXStore): IndexedSeq[Column] = {
    if (ps.getRowCount == 0) return IndexedSeq[Column]();

    var tbl = collection.mutable.IndexedSeq[IndexedSeq[Elem]]();

    for (i <- 0 until ps.getPAXPage(0).size) {
      val column = IndexedSeq[Elem]();
      tbl = tbl :+ column;
    }
    val elemPerMiniPage = 4;
    val n = ps.getRowCount.asInstanceOf[Int] / elemPerMiniPage + (if (ps.getRowCount.asInstanceOf[Int] % elemPerMiniPage == 0) 0 else 1);


    var page: PAXPage = null;
    for (i <- 0 until n) {
      page = ps.getPAXPage(i);
      for (j <- 0 until page.size) {
        tbl(j) = tbl(j) ++ page(j);
      }
    }

    var output = IndexedSeq[Column]();
    for (i <- 0 until tbl.size) {
      output = output :+ tbl(i);
    }

    return output;
  }

  def scan(tbl: RelOptTable, tableToStore: ScannableTable => Store): IndexedSeq[Column] = {
    val store = tableToStore(tbl.unwrap(classOf[ScannableTable]))
    table = tbl;

    return store match {
      case cs: ColumnStore => getTableColumnStore(cs);
      case rs: RowStore => getTableRowStore(rs);
      case ps: PAXStore => getTablePaxStore(ps);
      case _ => null;
    }
  }

}
