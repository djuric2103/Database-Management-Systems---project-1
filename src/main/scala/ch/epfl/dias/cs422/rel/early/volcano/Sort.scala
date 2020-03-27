package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

import ch.epfl.dias.cs422.rel.common.Sorting


class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var table : IndexedSeq[Tuple] = null;
  var curr : Iterator[Tuple] = null;

  def getInt(x : Any): Int ={
    x match {
      case i: Int => i
      case _ => 0
    }
  }

  /*def compare(a: Tuple, b: Tuple): Int = {
    for(j <- 0 until collation.getFieldCollations.size()){
      val i = collation.getFieldCollations.get(j);
      val com = RelFieldCollation.compare(a(i.getFieldIndex).asInstanceOf[Comparable[_]], b(i.getFieldIndex).asInstanceOf[Comparable[_]],42);

      val t = i.getDirection match {
        case RelFieldCollation.Direction.ASCENDING | RelFieldCollation.Direction.STRICTLY_ASCENDING | RelFieldCollation.Direction.CLUSTERED => 1;
        case RelFieldCollation.Direction.DESCENDING | RelFieldCollation.Direction.STRICTLY_DESCENDING => -1;
      }

      if(com != 0) {
        return t * com;
      }
    }
    return 0;
  }*/

  override def open(): Unit = {
    table = input.toIndexedSeq;

    val of = if (offset != null) getInt(evalLiteral(offset)) else 0;
    val fet = if (fetch != null) getInt(evalLiteral(fetch)) else table.size;

    table = Sorting.sort(table,collation, of,fet);

/*
    table = table.sortWith(compare(_, _) < 0);

    if(of != 0 || fet != table.size){
      var temp = IndexedSeq[Tuple]();
      for(i <- of until Math.min(table.size, of + fet)){
        temp = temp :+ table(i);
      }
      table = temp;
    }*/
    curr = table.iterator;
  }

  override def next(): Tuple = {
    if(curr.hasNext)
      return curr.next();
    return null;
  }

  override def close(): Unit = {
    curr = null;
    table = null;
  }
}
