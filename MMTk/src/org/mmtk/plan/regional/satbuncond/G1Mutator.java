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
package org.mmtk.plan.regional.satbuncond;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.Region;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

/**
 * This class implements <i>per-mutator thread</i> behavior
 * and state for the <i>RegionalCopy</i> plan, which implements a full-heap
 * semi-space collector.<p>
 *
 * Specifically, this class defines <i>RegionalCopy</i> mutator-time allocation
 * and per-mutator thread collection semantics (flushing and restoring
 * per-mutator allocator state).<p>
 *
 * See {@link G1} for an overview of the semi-space algorithm.
 *
 * @see G1
 * @see G1Collector
 * @see StopTheWorldMutator
 * @see MutatorContext
 */
@Uninterruptible
public class G1Mutator extends  org.mmtk.plan.regional.RegionalMutator {

  protected final RegionAllocator ra;
  private final ObjectReferenceDeque modbuf;
  static boolean newMutatorBarrierActive = false;
  boolean barrierActive = true;

  public G1Mutator() {
    super();
    ra = new RegionAllocator(G1.regionSpace, Region.NORMAL);
    modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
  }

  @Override
  public void collectionPhase(short phaseId, boolean primary) {
    //Log.write("[Mutator] ");
    //Log.writeln(Phase.getName(phaseId));
    if (phaseId == G1.PREPARE) {
      barrierActive = false;
      modbuf.reset();
      super.collectionPhase(G1.PREPARE, primary);
      return;
    }

    if (phaseId == G1.EVACUATE_RELEASE) {
      super.collectionPhase(G1.EVACUATE_RELEASE, primary);
      barrierActive = true;
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Inline
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (ref.isNull()) return;
//    if (HeaderByte.attemptLog(ref)) {
      modbuf.insert(ref);
//    }
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    if (barrierActive) checkAndEnqueueReference(slot.loadObjectReference());
    VM.barriers.objectReferenceWrite(src, tgt, metaDataA, metaDataB, mode);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old,
                                                  ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, tgt, metaDataA, metaDataB, mode);
    if (barrierActive) checkAndEnqueueReference(old);
    return result;
  }

  @Inline
  @Override
  public ObjectReference javaLangReferenceReadBarrier(ObjectReference ref) {
    if (barrierActive) checkAndEnqueueReference(ref);
    return ref;
  }

  @Inline
  G1 global() {
    return (G1) VM.activePlan.global();
  }
}
