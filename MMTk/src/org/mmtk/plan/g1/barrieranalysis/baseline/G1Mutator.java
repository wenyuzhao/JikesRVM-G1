/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */
package org.mmtk.plan.g1.barrieranalysis.baseline;

import org.vmmagic.pragma.*;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.Card;
import org.mmtk.policy.region.CardTable;
import org.mmtk.policy.region.RegionSpace;
import org.mmtk.utility.Constants;
import org.mmtk.vm.VM;
import org.vmmagic.unboxed.*;

@Uninterruptible
public class G1Mutator extends org.mmtk.plan.g1.G1Mutator {
  @NoInline
  @Override
  protected void cardMarkingBarrier(ObjectReference src) {
    if (G1.MEASURE_TAKERATE) G1.barrierFast.inc(1);
    int index = CardTable.getIndex(src);
    if (CardTable.get(index) == Card.NOT_DIRTY) {
      if (G1.MEASURE_TAKERATE) G1.barrierSlow.inc(1);
      CardTable.set(index, Card.DIRTY);
      rsEnqueue(Word.fromIntZeroExtend(index).lsh(Card.LOG_BYTES_IN_CARD).toAddress());
    }
  }

  @NoInline
  private void rsEnqueue(Address card) {
    if (dirtyCardQueue.isZero()) acquireDirtyCardQueue();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(dirtyCardQueueCursor.plus(4).LE(dirtyCardQueueLimit));
    dirtyCardQueueCursor.store(card);
    dirtyCardQueueCursor = dirtyCardQueueCursor.plus(Constants.BYTES_IN_ADDRESS);
    if (dirtyCardQueueCursor.GE(dirtyCardQueueLimit)) {
      dirtyCardQueueCursor = dirtyCardQueue;
    }
  }

  @Inline
  protected void xorBarrier(ObjectReference src, Address slot, ObjectReference ref) {
    if (isCrossRegionRef(src, slot, ref)) {
      cardMarkingBarrier(src);
    }
  }

  @Inline
  protected boolean isCrossRegionRef(ObjectReference src, Address slot, ObjectReference obj) {
    if (obj.isNull()) return false;
    Word x = slot.toWord();
    Word y = VM.objectModel.refToAddress(obj).toWord();
    return !x.xor(y).rshl(Region.LOG_BYTES_IN_REGION).isZero();
  }
}
