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
package org.mmtk.plan.regional.remsetbarrier;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.*;

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
public class G1Mutator extends org.mmtk.plan.regional.RegionalMutator {
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

  @NoInline
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
    remSetLogBuffer = G1.offHeapMetaDataSpace.acquire(1);
    remSetLogBufferCursor = 0;
  }

  @NoInline
  public void markAndEnqueueCard(ObjectReference src) {
    Address card = Region.Card.of(src);
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
        markAndEnqueueCard(src);
      }
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.objectReferenceWrite(src, value, metaDataA, metaDataB, mode);
    checkCrossRegionPointer(src, slot, value);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, value, metaDataA, metaDataB, mode);
    checkCrossRegionPointer(src, slot, value);
    return result;
  }
}
