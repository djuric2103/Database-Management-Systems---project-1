package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.rel.common.Sorting
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var table: IndexedSeq[Tuple] = null;
  var curr: Iterator[Tuple] = null;

  def getInt(x: RexNode): Int = {
    evalLiteral(x) match {
      case i: Int => i
      case _ => 0
    }
  }

  override def open(): Unit = {
    table = BlockConvert.toIndSeq(input.iterator);

    val of = if (offset != null) getInt(offset) else 0;
    val fet = if (fetch != null) getInt(fetch) else table.size;

    table = Sorting.sort(table, collation, of, fet);
    curr = table.iterator;
  }

  override def next(): Block = {
    return BlockConvert.getNext(curr, blockSize);
  }

  override def close(): Unit = {
    curr = null;
    table = null;
  }
}
