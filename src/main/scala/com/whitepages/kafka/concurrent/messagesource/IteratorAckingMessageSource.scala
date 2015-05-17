package com.whitepages.kafka.concurrent.messagesource

import com.whitepages.kafka.concurrent.BatchAckingMessageSource

// Mostly for use in test situations, as the "commit" semantic has no real meaning here.
// (although one could imagine giving it a meaning by using a buffer)
class IteratorAckingMessageSource[T](msgs: Iterator[T]) extends BatchAckingMessageSource[T] {
  var commitPointer = 0
  var fetchPointer = 0

  override def commit(): Unit = commitPointer = fetchPointer
  override def nextBatch(): Seq[T] = {
    if (msgs.hasNext) {
      val next = msgs.next()
      fetchPointer = fetchPointer + 1
      Seq(next)
    }
    else {
      Seq()
    }
  }
}

