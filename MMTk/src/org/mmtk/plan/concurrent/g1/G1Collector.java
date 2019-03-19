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
package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.*;
import org.mmtk.plan.concurrent.ConcurrentCollector;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.pragma.Unpreemptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements <i>per-collector thread</i> behavior
 * and state for the <i>RegionalCopy</i> plan, which implements a full-heap
 * semi-space collector.<p>
 *
 * Specifically, this class defines <i>RegionalCopy</i> collection behavior
 * (through <code>trace</code> and the <code>collectionPhase</code>
 * method), and collection-time allocation (copying of objects).<p>
 *
 * See {@link G1} for an overview of the semi-space algorithm.
 *
 * @see G1
 * @see G1Mutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class G1Collector extends ConcurrentCollector {

  /****************************************************************************
   * Instance fields
   */

  /**
   *
   */
  protected final RegionAllocator g1CopySurvivor = new RegionAllocator(G1.regionSpace, Region.SURVIVOR);
  protected final RegionAllocator g1CopyOld = new RegionAllocator(G1.regionSpace, Region.OLD);
  protected final G1MarkTraceLocal markTrace;// = new G1MarkTraceLocal(global().markTrace);
  protected final G1NurseryTraceLocal nurseryTrace = new G1NurseryTraceLocal(global().nurseryTrace);
  protected final G1MatureTraceLocal matureTrace = new G1MatureTraceLocal(global().matureTrace);
  protected final EvacuationLinearScan evacuationLinearScan = new EvacuationLinearScan();
  protected int currentTrace = 0;
  public static final int TRACE_MARK = 0;
  public static final int TRACE_NURSERY = 1;
  public static final int TRACE_MATURE = 2;
  private static final Atomic.Int atomicCounter = new Atomic.Int();
  public final ObjectReferenceDeque modbuf;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public G1Collector() {
    modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
    markTrace = new G1MarkTraceLocal(global().markTrace, modbuf);//new CMSTraceLocal(global().msTrace, modbuf);
  }

  /****************************************************************************
   *
   * Collection-time allocation
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address allocCopy(ObjectReference original, int bytes, int align, int offset, int allocator) {
    if (allocator == G1.ALLOC_SURVIVOR) {
      return g1CopySurvivor.alloc(bytes, align, offset);
    } else {
      return g1CopyOld.alloc(bytes, align, offset);
    }
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    Region.Card.updateCardMeta(object);
    G1.regionSpace.postCopy(object, bytes);
  }

  /****************************************************************************
   *
   * Collection
   */

  public static Lock lock = VM.newLock("collectionPhaseLock");
  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    if (Region.verbose()) Log.writeln(Phase.getName(phaseId));
    if (phaseId == G1.PREPARE) {
      currentTrace = TRACE_MARK;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.CLOSURE) {
      G1.currentGCKind = G1.FULL_GC;
      markTrace.completeTrace();
      return;
    }

    if (phaseId == G1.RELEASE) {
      markTrace.completeTrace();
      markTrace.release();
      g1CopySurvivor.reset();
      g1CopyOld.reset();
      return;
    }

    if (phaseId == G1.RELOCATION_SET_SELECTION) {
      if (primary) {
        AddressArray blocksSnapshot = G1.regionSpace.snapshotRegions(G1.currentGCKind == G1.YOUNG_GC);

        if (G1.currentGCKind == G1.YOUNG_GC) {
          // blocksSnapshot is already and only contains young & survivor regions
          G1.relocationSet = blocksSnapshot;
        } else {
          G1.relocationSet = RegionSpace.computeRelocationRegions(blocksSnapshot, true, false);
        }
        if (G1.currentGCKind == G1.MIXED_GC) {
          PauseTimePredictor.predict(G1.relocationSet);
        }
        RegionSpace.markRegionsAsRelocate(G1.relocationSet);
      }
      rendezvous();
      return;
    }

    if (phaseId == G1.EAGER_CLEANUP) {
      RemSet.cleanupRemSetRefsToRelocationSet(G1.regionSpace, G1.relocationSet, true);
      atomicCounter.set(0);
      rendezvous();

      int index;
      while ((index = atomicCounter.add(1)) < G1.relocationSet.length()) {
        Address region = G1.relocationSet.get(index);
        if (!region.isZero() && Region.usedSize(region) == 0) {
          G1.relocationSet.set(index, Address.zero());
          G1.regionSpace.release(region);
        }
      }
      return;
    }

    if (phaseId == G1.EVACUATE) {
      g1CopySurvivor.reset();
      g1CopyOld.reset();
      evacuationLinearScan.evacuateRegions();
      return;
    }

    if (phaseId == G1.REFINE_CARDS) {
      ConcurrentRemSetRefinement.refineAllDirtyCards();
      return;
    }

    if (phaseId == G1.FORWARD_PREPARE) {
      currentTrace = global().nurseryGC() ? TRACE_NURSERY : TRACE_MATURE;
      getCurrentTrace().prepare();
      g1CopySurvivor.reset();
      g1CopyOld.reset();
      if (global().nurseryGC()) {
        nurseryTrace.evacuationAccumulator.reset();
        super.collectionPhase(G1.PREPARE, primary);
      }
      return;
    }

    if (phaseId == G1.REMEMBERED_SETS) {
      ((G1EvacuationTraceLocal) getCurrentTrace()).processRemSets();
      return;
    }

    if (phaseId == G1.FORWARD_CLOSURE) {
      getCurrentTrace().completeTrace();
      return;
    }

    if (phaseId == G1.FORWARD_RELEASE) {
      g1CopySurvivor.reset();
      g1CopyOld.reset();
      getCurrentTrace().release();
      if (global().nurseryGC()) {
        nurseryTrace.evacuationAccumulator.flush();
      }
      super.collectionPhase(G1.RELEASE, primary);
      return;
    }

    if (phaseId == G1.CLEAR_CARD_META) {
      Region.Card.clearCardMetaForUnmarkedCards(G1.regionSpace, false, global().nurseryGC());
      return;
    }

    if (phaseId == G1.CLEANUP) {
//      if (concurrentCleanupExecuted) return;
      RemSet.cleanupRemSetRefsToRelocationSet(G1.regionSpace, G1.relocationSet, false);
      G1.regionSpace.cleanupRegions(G1.relocationSet, false);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  protected boolean concurrentTraceComplete() {
    if (!global().markTrace.hasWork()) {
      return true;
    }
    return false;
  }

  @Override
  @Unpreemptible
  public void concurrentCollectionPhase(short phaseId) {
    if (Region.verbose()) Log.writeln(Phase.getName(phaseId));

    if (phaseId == G1.CONCURRENT_CLOSURE) {
      currentTrace = TRACE_MARK;
      super.concurrentCollectionPhase(phaseId);
    }
  }

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>RegionalCopy</code> instance. */
  @Inline
  private static G1 global() {
    return (G1) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    switch (currentTrace) {
      case TRACE_MARK: return markTrace;
      case TRACE_NURSERY: return nurseryTrace;
      case TRACE_MATURE: return matureTrace;
      default: return null;
    }
  }
}
