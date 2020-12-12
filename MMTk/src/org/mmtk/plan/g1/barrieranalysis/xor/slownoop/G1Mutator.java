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
package org.mmtk.plan.g1.barrieranalysis.xor.slownoop;

import org.mmtk.policy.region.Region;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;
import org.mmtk.policy.region.Card;
import org.mmtk.policy.region.CardTable;

@Uninterruptible
public class G1Mutator extends org.mmtk.plan.g1.barrieranalysis.xor.G1Mutator {
  @Inline
  @Override
  protected boolean isCrossRegionRef(ObjectReference src, Address slot, ObjectReference obj) {
    if (obj.isNull()) return false;
    Word x = slot.toWord();
    Word y = VM.objectModel.refToAddress(obj).toWord();
    return !x.xor(y).rshl(Region.LOG_BYTES_IN_REGION).isZero();
  }

  @Inline
  @Override
  protected void cardMarkingBarrier(ObjectReference src) {
    int index = CardTable.getIndex(src);
    if (CardTable.get(index) == Card.DIRTY) {
      CardTable.set(index, Card.NOT_DIRTY);
      rsEnqueue(Word.fromIntZeroExtend(index).lsh(Card.LOG_BYTES_IN_CARD).toAddress());
    }
  }
}
