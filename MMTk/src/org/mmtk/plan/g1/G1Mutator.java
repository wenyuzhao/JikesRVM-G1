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
package org.mmtk.plan.g1;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Phase;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.region.Card;
import org.mmtk.policy.region.CardTable;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.Space;
import org.mmtk.policy.region.RegionSpace;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.BumpPointer2;
import org.mmtk.utility.alloc.RegionAllocator2;
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
public class G1Mutator extends StopTheWorldMutator {

  public static boolean newMutatorBarrierActive = false;
  protected volatile boolean barrierActive = newMutatorBarrierActive;
  protected final ObjectReferenceDeque modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
  protected final RegionAllocator2 g1 = new RegionAllocator2(G1.regionSpace, G1.ENABLE_GENERATIONAL_GC ? Region.EDEN : Region.OLD);
  protected final BumpPointer2 immortal2 = new BumpPointer2(Plan.immortalSpace);
  protected Address dirtyCardQueue = Address.zero();
  protected Address dirtyCardQueueCursor = Address.zero();
  protected Address dirtyCardQueueLimit = Address.zero();

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address alloc(int bytes, int align, int offset, int allocator, int site) {
    if (VM.VERIFY_ASSERTIONS) {
      switch (allocator) {
        case G1.ALLOC_G1_EDEN:
        case G1.ALLOC_G1_SURVIVOR:
        case G1.ALLOC_G1_OLD:
          VM.assertions.fail("Unreachable");
          break;
        default: break;
      }
    }
    switch (allocator) {
      case G1.ALLOC_DEFAULT: return g1.alloc(bytes, align, offset);
      case G1.ALLOC_LOS:     return los.alloc(bytes, align, offset);
      default:               return immortal2.alloc(bytes, align, offset);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference ref, ObjectReference typeRef, int bytes, int allocator) {
    switch (allocator) {
      case G1.ALLOC_DEFAULT: return;
      case G1.ALLOC_LOS:     G1.loSpace.initializeHeader(ref, true); return;
      default:               G1.immortalSpace.initializeHeader(ref);  return;
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == G1.regionSpace) return g1;
    if (space == Plan.loSpace)   return los;
    return immortal2;
  }

  /****************************************************************************
   *
   * Collection
   */

  /**
   * {@inheritDoc}
   */
  @Override
  public void collectionPhase(short phaseId, boolean primary) {
    if (G1.VERBOSE) {
      Log.write("Mutator ");
      Log.writeln(Phase.getName(phaseId));
    }
    if (phaseId == G1.PREPARE) {
      g1.reset();
      immortal2.reset();
      VM.memory.collectorPrepareVMSpace();
      modbuf.reset();
      return;
    }

    if (phaseId == G1.RELEASE) {
      g1.reset();
      immortal2.reset();
      VM.memory.collectorReleaseVMSpace();
      modbuf.flushLocal();
      return;
    }

    if (phaseId == G1.REFINE_CARDS) {
      g1.reset();
      immortal2.reset();
      flushDirtyCardQueue();
      // TODO: Clear dirty card queue
      return;
    }

    if (phaseId == G1.EVACUATE_PREPARE) {
      g1.adjustTLABSize();
      g1.reset();
      immortal2.reset();
      VM.memory.collectorPrepareVMSpace();
      return;
    }

    if (phaseId == G1.EVACUATE_RELEASE) {
      g1.reset();
      immortal2.reset();
      VM.memory.collectorReleaseVMSpace();
      return;
    }

    if (phaseId == G1.SET_BARRIER_ACTIVE) {
      barrierActive = true;
      return;
    }

    if (phaseId == G1.CLEAR_BARRIER_ACTIVE) {
      barrierActive = false;
      return;
    }

    if (phaseId == G1.FLUSH_MUTATOR) {
      flush();
      return;
    }

    if (phaseId == Validation.VALIDATE_PREPARE) {
      g1.reset();
      immortal2.reset();
      return;
    }

    if (phaseId == Validation.VALIDATE_RELEASE) {
      g1.reset();
      immortal2.reset();
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void deinitMutator() {
    flush();
    flushDirtyCardQueue();
  }

  @Override
  public void flushRememberedSets() {
    g1.reset();
    immortal2.reset();
    if (G1.ENABLE_CONCURRENT_MARKING) modbuf.flushLocal();
//    flushDirtyCardQueue();
    assertRemsetsFlushed();
  }

  @Inline
  protected void checkAndEnqueueReference(ObjectReference ref) {
    // if (!ref.isNull() && G1.attemptLog(ref)) {
    if (!ref.isNull()) {
      modbuf.insert(ref);
    }
  }

  @Inline
  protected void cardMarkingBarrier(ObjectReference src) {
    if (!G1.ENABLE_REMEMBERED_SETS) return;
    int index = CardTable.getIndex(src);
    if (CardTable.get(index) == Card.NOT_DIRTY) {
      CardTable.set(index, Card.DIRTY);
      rsEnqueue(Word.fromIntZeroExtend(index).lsh(Card.LOG_BYTES_IN_CARD).toAddress());
    }
  }

  @NoInline
  protected void cardMarkingBarrierOutOfLine(ObjectReference src) {
    cardMarkingBarrier(src);
  }

  @Inline
  protected void xorBarrier(ObjectReference src, Address slot, ObjectReference ref) {
    if (RegionSpace.isCrossRegionRef(src, slot, ref)) {
      cardMarkingBarrierOutOfLine(src);
    }
  }

  protected void acquireDirtyCardQueue() {
    dirtyCardQueue = CardRefinement.filledRSBufferQueue.allocateLocalQueue();
    dirtyCardQueueCursor = dirtyCardQueue;
    dirtyCardQueueLimit = dirtyCardQueue.plus(CardRefinement.filledRSBufferQueue.LOCAL_BUFFER_SIZE);
  }

  @Inline
  private void rsEnqueue(Address card) {
    if (dirtyCardQueue.isZero()) acquireDirtyCardQueue();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(dirtyCardQueueCursor.plus(4).LE(dirtyCardQueueLimit));
    dirtyCardQueueCursor.store(card);
    dirtyCardQueueCursor = dirtyCardQueueCursor.plus(Constants.BYTES_IN_ADDRESS);
    if (dirtyCardQueueCursor.GE(dirtyCardQueueLimit)) {
      flushDirtyCardQueue();
    }
  }

  @NoInline
  private void flushDirtyCardQueue() {
    if (!dirtyCardQueue.isZero()) {
      CardRefinement.filledRSBufferQueue.enqueue(dirtyCardQueue);
      dirtyCardQueue = Address.zero();
      dirtyCardQueueCursor = Address.zero();
      dirtyCardQueueLimit = Address.zero();
    }
  }

//  @Inline
//  private void resetDirtyCardQueue() {
//    if (!dirtyCardQueue.isZero()) {
//      CardRefinement.filledRSBufferQueue.release(dirtyCardQueue);
//      dirtyCardQueue = Address.zero();
//      dirtyCardQueueCursor = Address.zero();
//      dirtyCardQueueLimit = Address.zero();
//    }
//  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    if (G1.ENABLE_CONCURRENT_MARKING && barrierActive) checkAndEnqueueReference(slot.loadObjectReference());
    VM.barriers.objectReferenceWrite(src, tgt, metaDataA, metaDataB, mode);
    if (G1.USE_XOR_BARRIER) {
      xorBarrier(src, slot, tgt);
    } else {
      cardMarkingBarrier(src);
    }
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    if (G1.ENABLE_CONCURRENT_MARKING && barrierActive) checkAndEnqueueReference(old);
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, tgt, metaDataA, metaDataB, mode);
    if (result) {
      if (G1.USE_XOR_BARRIER) {
        xorBarrier(src, slot, tgt);
      } else {
        cardMarkingBarrier(src);
      }
    }
    return result;
  }

  @Inline
  @Override
  public ObjectReference javaLangReferenceReadBarrier(ObjectReference ref) {
    if (G1.ENABLE_CONCURRENT_MARKING && barrierActive) checkAndEnqueueReference(ref);
    return ref;
  }

  @Inline
  G1 global() {
    return (G1) VM.activePlan.global();
  }
}
