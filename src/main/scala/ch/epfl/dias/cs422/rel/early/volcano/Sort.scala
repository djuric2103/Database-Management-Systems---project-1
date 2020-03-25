package ch.epfl.dias.cs422.rel.early.volcano


import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

import scala.util.Sorting.quickSort


class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var table = new Array[Tuple](input.size);
  var ind = 0;
  var n = input.size;

  val of : Int = evalLiteral(offset).toString.toInt;
  val fet : Int = evalLiteral(fetch).toString.toInt;

  def compare(a: Tuple, b: Tuple): Int = {
    for(j <- 0 until collation.getFieldCollations.size()){
      val i = collation.getFieldCollations.get(j);
      val com = RelFieldCollation.compare(a(i.getFieldIndex).asInstanceOf[Comparable[_]], b(i.getFieldIndex).asInstanceOf[Comparable[_]],42);
      /*val t : Int = i.getDirection match {
        case RelFieldCollation.Direction.ASCENDING | RelFieldCollation.Direction.STRICTLY_ASCENDING | RelFieldCollation.Direction.CLUSTERED => com;
        case RelFieldCollation.Direction.DESCENDING | RelFieldCollation.Direction.STRICTLY_DESCENDING => 0-com;
      }*/
      var t = 1;
      if(i.getDirection.isDescending){
        t = -1;
      }

      println(a(i.getFieldIndex) + "\t"+b(i.getFieldIndex) + "\t rezultat "+com);
      if(com != 0) return t*com;
    }
    return 0;
  }

  override def open(): Unit = {
    input.open();
    for(i <- 0 until n){
      val row = input.next();
      table(i) =  row;
    }


    quickSort [Tuple](table : Array[Tuple])((a : Tuple, b : Tuple) =>  compare(a,b))
  }

  override def next(): Tuple = {
    if(of + ind < n && ind < fet){
      ind += 1;
      return table(of + ind  -1);
    }
    return null;
  }

  override def close(): Unit = {
    //table = null;
  }
}
