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
package org.mmtk.plan.regional.allbarriers;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.utility.Constants;
import org.mmtk.utility.HeaderByte;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

import static org.mmtk.utility.Constants.LOG_BYTES_IN_ADDRESS;

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
    if (HeaderByte.attemptLog(ref)) {
      modbuf.insert(ref);
    }
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    if (barrierActive) checkAndEnqueueReference(slot.loadObjectReference());
    VM.barriers.objectReferenceWrite(src, tgt, metaDataA, metaDataB, mode);
    checkCrossRegionPointer(src, slot, tgt);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old,
                                                  ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, tgt, metaDataA, metaDataB, mode);
    if (barrierActive) checkAndEnqueueReference(old);
    checkCrossRegionPointer(src, slot, tgt);
    return result;
  }

  @Inline
  @Override
  public ObjectReference javaLangReferenceReadBarrier(ObjectReference ref) {
    if (barrierActive) checkAndEnqueueReference(ref);
    return ref;
  }

  @Override
  public void deinitMutator() {
    enqueueCurrentRSBuffer(true, false);
    super.deinitMutator();
  }

  protected static final int REMSET_LOG_BUFFER_SIZE = Constants.BYTES_IN_PAGE >>> Constants.LOG_BYTES_IN_ADDRESS;
  protected Address remSetLogBuffer = Address.zero();
  protected int remSetLogBufferCursor = 0;

  @Inline
  protected Address remSetLogBuffer() {
    if (remSetLogBuffer.isZero())
      remSetLogBuffer = Plan.offHeapMetaDataSpace.acquire(1);
    return remSetLogBuffer;
  }

  static AddressArray globalRemSetLogBuffer = AddressArray.create(512);
  static int globalRemSetLogBufferCursor = 0;
  Lock globalRemSetLogBufferLock = VM.newLock("globalRemSetLogBufferLock");

  public void enqueueCurrentRSBuffer(boolean triggerConcurrentRefinement, boolean acquireNewBuffer) {
    if (remSetLogBufferCursor == 0) return;
    globalRemSetLogBufferLock.acquire();
    // Add to globalRemSetLogBuffer
    if (globalRemSetLogBufferCursor >= globalRemSetLogBuffer.length()) {
      globalRemSetLogBufferCursor = 0;
    }
    globalRemSetLogBuffer.set(globalRemSetLogBufferCursor, remSetLogBuffer);
    globalRemSetLogBufferCursor += 1;
    globalRemSetLogBufferLock.release();
    // Acquire new buffer
    remSetLogBuffer = org.mmtk.plan.regional.remsetbarrier.G1.offHeapMetaDataSpace.acquire(1);
    remSetLogBufferCursor = 0;
  }

  @NoInline
  public void markAndEnqueueCard(Address card) {
    if (CardTable.attemptToMarkCard(card, true)) {
      remSetLogBuffer().plus(remSetLogBufferCursor << LOG_BYTES_IN_ADDRESS).store(card);
      remSetLogBufferCursor += 1;
      if (remSetLogBufferCursor >= REMSET_LOG_BUFFER_SIZE) {
        enqueueCurrentRSBuffer(true, true);
      }
    }
  }

  @Inline
  public void checkCrossRegionPointer(ObjectReference src, Address slot, ObjectReference ref) {
    Word x = VM.objectModel.refToAddress(src).toWord();
    Word y = VM.objectModel.refToAddress(ref).toWord();
    Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
    tmp = ref.isNull() ? Word.zero() : tmp;
    if (!tmp.isZero()) {
      markAndEnqueueCard(Region.Card.of(src));
    }
  }

  @Inline
  G1 global() {
    return (G1) VM.activePlan.global();
  }
}
