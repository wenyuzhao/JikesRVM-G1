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
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.BumpPointer2;
import org.mmtk.utility.alloc.RegionAllocator2;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
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
  private final ObjectReferenceDeque modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
  protected final RegionAllocator2 g1 = new RegionAllocator2(G1.regionSpace, Region.NORMAL);
  protected final BumpPointer2 immortal2 = new BumpPointer2(Plan.immortalSpace);

  /****************************************************************************
   *
   * Mutator-time allocation
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address alloc(int bytes, int align, int offset, int allocator, int site) {
    switch (allocator) {
      case G1.ALLOC_G1:  return g1.alloc(bytes, align, offset);
      case G1.ALLOC_LOS: return los.alloc(bytes, align, offset);
      default:           return immortal2.alloc(bytes, align, offset);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference ref, ObjectReference typeRef, int bytes, int allocator) {
    switch (allocator) {
      case G1.ALLOC_G1:  return;
      case G1.ALLOC_LOS: G1.loSpace.initializeHeader(ref, true); return;
      default:           G1.immortalSpace.initializeHeader(ref);  return;
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == G1.regionSpace) return g1;
    return super.getAllocatorFromSpace(space);
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
      g1.adjustTLABSize();
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
      // TODO: Clear dirty card queue
      return;
    }

    if (phaseId == G1.EVACUATE_PREPARE) {
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

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    g1.reset();
    immortal2.reset();
    if (G1.ENABLE_CONCURRENT_MARKING) modbuf.flushLocal();
    assertRemsetsFlushed();
  }

  @Inline
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (!ref.isNull() && G1.attemptLog(ref)) {
      modbuf.insertOutOfLine(ref);
    }
  }

  @Inline
  void cardMarkingBarrier(ObjectReference src) {
    if (!G1.ENABLE_REMEMBERED_SETS) return;
    Address card = Card.of(src);

    if (CardTable.get(card) == Card.NOT_DIRTY) {
      CardTable.set(card, Card.DIRTY);
//      if super::ENABLE_CONCURRENT_REFINEMENT {
//        self.rs_enquene(card);
//      }
    }
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    if (barrierActive) checkAndEnqueueReference(slot.loadObjectReference());
    VM.barriers.objectReferenceWrite(src, tgt, metaDataA, metaDataB, mode);
    cardMarkingBarrier(src);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    if (barrierActive) checkAndEnqueueReference(old);
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, tgt, metaDataA, metaDataB, mode);
    cardMarkingBarrier(src);
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
