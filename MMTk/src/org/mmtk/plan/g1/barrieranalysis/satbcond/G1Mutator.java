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
package org.mmtk.plan.g1.barrieranalysis.satbcond;

import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class G1Mutator extends org.mmtk.plan.g1.barrieranalysis.baseline.G1Mutator {
  @Inline
  protected void checkAndEnqueueReference(ObjectReference ref) {
    // if (ref.isNull()) return;
    // if (G1.MEASURE_TAKERATE) G1.barrierFast.inc(1);
    // if (G1.isUnlogged(ref)) {
    //   if (G1.MEASURE_TAKERATE) G1.barrierSlow.inc(1);
    //   G1.markAsLogged(ref);
    //   modbuf.insertOutOfLine(ref);
    // }
    if (ref.isNull()) return;
    if (G1.attemptLog(ref)) {
      modbuf.insert(ref);
    }
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    checkAndEnqueueReference(slot.loadObjectReference());
    VM.barriers.objectReferenceWrite(src, tgt, metaDataA, metaDataB, mode);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, tgt, metaDataA, metaDataB, mode);
    checkAndEnqueueReference(old);
    return result;
  }

  @Inline
  @Override
  public ObjectReference javaLangReferenceReadBarrier(ObjectReference ref) {
    checkAndEnqueueReference(ref);
    return ref;
  }
}
