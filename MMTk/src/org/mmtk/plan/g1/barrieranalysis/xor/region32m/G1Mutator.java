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
package org.mmtk.plan.g1.barrieranalysis.xor.region32m;

import org.mmtk.policy.region.Region;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class G1Mutator extends org.mmtk.plan.g1.barrieranalysis.xor.G1Mutator {
  @Inline
  @Override
  protected boolean isCrossRegionRef(ObjectReference src, Address slot, ObjectReference obj) {
//    if (obj.isNull()) return false;
//    Word x = slot.toWord();
//    Word y = VM.objectModel.refToAddress(obj).toWord();
//    return !x.xor(y).rshl(Region.LOG_BYTES_IN_REGION + 5).isZero();

    if (obj.toAddress().NE(Address.zero())) {
      if (src.toAddress().toWord().xor(obj.toAddress().toWord()).GE(Word.fromIntZeroExtend(33554432))) {
        return true;
      }
    }
    return false;
  }
}
