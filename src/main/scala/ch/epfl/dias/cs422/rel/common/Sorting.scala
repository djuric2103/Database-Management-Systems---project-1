package ch.epfl.dias.cs422.rel.common

import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}


object Sorting{
  def compare(collation: RelCollation, a: Tuple, b: Tuple): Int = {
    for (j <- 0 until collation.getFieldCollations.size()) {
      val i = collation.getFieldCollations.get(j);
      val com = RelFieldCollation.compare(a(i.getFieldIndex).asInstanceOf[Comparable[_]], b(i.getFieldIndex).asInstanceOf[Comparable[_]], 42);

      val t = i.getDirection match {
        case RelFieldCollation.Direction.ASCENDING | RelFieldCollation.Direction.STRICTLY_ASCENDING | RelFieldCollation.Direction.CLUSTERED => 1;
        case RelFieldCollation.Direction.DESCENDING | RelFieldCollation.Direction.STRICTLY_DESCENDING => -1;
      }

      if (com != 0) {
        return t * com;
      }
    }
    return 0;
  }

  def crop(tbl: IndexedSeq[IndexedSeq[Any]], offset : Int, fetch : Int) : IndexedSeq[IndexedSeq[Any]] = {
    if (offset != 0 || fetch != tbl.size) {
      var temp = IndexedSeq[IndexedSeq[Any]]();
      for (i <- offset until Math.min(tbl.size, offset + fetch)) {
        temp = temp :+ tbl(i);
      }
      return temp;
    }
    return tbl;
  }

  def sort(tbl: IndexedSeq[Tuple], collation: RelCollation, offset: Int, fetch: Int): IndexedSeq[Tuple] = {
    val table = tbl.sortWith(compare(collation,_, _) < 0);
    return crop(table, offset, fetch);
  }
}
