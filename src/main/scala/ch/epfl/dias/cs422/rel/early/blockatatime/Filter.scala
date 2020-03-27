package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  lazy val iter : Iterator[Block] = input.iterator;
  var currentBlock = IndexedSeq[Tuple]();
  var currentIndex = blockSize;

  override def open(): Unit = {
  }

  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def next(): Block = {
    if(currentIndex == currentBlock.size && !iter.hasNext) return null;
    var output = IndexedSeq[Tuple]();

    while(output.size < blockSize && (currentIndex < currentBlock.size || iter.hasNext)){
      if(currentIndex >= currentBlock.size){
        currentBlock = iter.next();
        currentIndex = 0;
      }
      if(e(currentBlock(currentIndex)) == true) {
        output = output :+ currentBlock(currentIndex);
      }
      currentIndex += 1;
    }

    return if(output.size != 0) output else null;
  }

  override def close(): Unit = {
    currentBlock = null;
    currentIndex = blockSize;
  }
}
