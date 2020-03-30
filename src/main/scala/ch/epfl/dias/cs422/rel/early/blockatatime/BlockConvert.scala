package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}

object BlockConvert {
  def getNext(curr: Iterator[Tuple], blockSize: Int): Block = {
    if (!curr.hasNext) return null;
    var block = IndexedSeq[Tuple]();
    while (block.size < blockSize && curr.hasNext) {
      block = block :+ curr.next();
    }
    return block;
  }

  def toIndSeq(iterator: Iterator[Block]): IndexedSeq[Tuple] = {
    var table = IndexedSeq[Tuple]();
    while (iterator.hasNext) {
      table = table ++ iterator.next();
    }
    return table;
  }
}
