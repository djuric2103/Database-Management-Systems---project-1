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

  def sort(tbl: IndexedSeq[Tuple], collation: RelCollation, offset: Int, fetch: Int): IndexedSeq[Tuple] = {
    var table = tbl.sortWith(compare(collation,_, _) < 0);

    if (offset != 0 || fetch != table.size) {
      var temp = IndexedSeq[Tuple]();
      for (i <- offset until Math.min(table.size, offset + fetch)) {
        temp = temp :+ table(i);
      }
      table = temp;
    }
    return table;
  }
}
