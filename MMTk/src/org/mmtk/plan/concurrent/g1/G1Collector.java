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
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.pragma.Unpreemptible;
import org.vmmagic.unboxed.Address;
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
  protected final RegionAllocator copy = new RegionAllocator(G1.regionSpace, true);
  protected final G1MarkTraceLocal markTrace = new G1MarkTraceLocal(global().markTrace);
  protected final G1RedirectTraceLocal redirectTrace = new G1RedirectTraceLocal(global().redirectTrace);
  protected TraceLocal currentTrace;
  protected RemSet.Builder remSetBuilder = new RemSet.Builder(markTrace, G1.regionSpace);

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public G1Collector() {}

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
    return copy.alloc(bytes, align, offset);
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
    if (VM.VERIFY_ASSERTIONS) Log.writeln(Phase.getName(phaseId));
    if (phaseId == G1.PREPARE) {
      currentTrace = markTrace;
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
      VM.assertions.fail("Unreachable");
      markTrace.completeTrace();
      markTrace.release();
      return;
    }

    if (phaseId == G1.EAGER_CLEANUP_BLOCKS) {
      G1.regionSpace.releaseZeroRegions(G1.relocationSet, false);
      if (rendezvous() == 0) {
        int cursor = 0;
        for (int i = 0; i < G1.relocationSet.length(); i++) {
          Address a = G1.relocationSet.get(i);
          if (!a.isZero()) {
            G1.relocationSet.set(cursor++, a);
          }
        }
        while (cursor < G1.relocationSet.length()) {
          G1.relocationSet.set(cursor++, Address.zero());
        }
      }
      rendezvous();
      return;
    }

    if (phaseId == G1.REDIRECT_PREPARE) {
      ConcurrentRemSetRefinement.refineAllDirtyCards();
      rendezvous();
//      if (global().isCurrentGCNursery()) super.collectionPhase(G1.PREPARE, primary);
      currentTrace = redirectTrace;
      redirectTrace.prepare();
      copy.reset();
      if (global().nurseryGC()) {
//        VM.memory.collectorPrepareVMSpace();
        super.collectionPhase(G1.PREPARE, primary);
      }
      return;
    }

    if (phaseId == G1.REMEMBERED_SETS) {
      Region.Card.DISABLE_DYNAMIC_HASH_OFFSET = true;
      ConcurrentRemSetRefinement.refineAllDirtyCards();
      Region.Card.DISABLE_DYNAMIC_HASH_OFFSET = false;
      redirectTrace.processRemSets();
      return;
    }

    if (phaseId == G1.FINALIZABLE) {
      if (global().nurseryGC()) {
        redirectTrace.nurseryTraceFinalizables = true;
        rendezvous();
      }
    }

    if (phaseId == G1.PHANTOM_REFS) {
      if (global().nurseryGC()) {
        redirectTrace.nurseryTraceFinalizables = false;
        rendezvous();
      }
    }

    if (phaseId == G1.REDIRECT_CLOSURE) {
      redirectTrace.completeTrace();
      return;
    }

    if (phaseId == G1.REDIRECT_RELEASE) {
      copy.reset();
      redirectTrace.release();
      if (!global().nurseryGC()) {
        markTrace.release();
        super.collectionPhase(G1.RELEASE, primary);
      } else {
        super.collectionPhase(G1.RELEASE, primary);
//        VM.memory.collectorReleaseVMSpace();
      }
      return;
    }

    if (phaseId == G1.CLEAR_CARD_META) {
      Region.Card.clearCardMetaForUnmarkedCards(G1.regionSpace, false);
      return;
    }

    if (phaseId == G1.CLEANUP_BLOCKS) {
      RemSet.cleanupRemSetRefsToRelocationSet(G1.regionSpace, G1.relocationSet, false);
      rendezvous();
      G1.regionSpace.cleanupBlocks(G1.relocationSet, false);
      rendezvous();
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

//  static

  @Override
  @Unpreemptible
  public void concurrentCollectionPhase(short phaseId) {
    Log.writeln(Phase.getName(phaseId));
    if (phaseId == G1.CONCURRENT_CLOSURE) {
      currentTrace = markTrace;
    }
    super.concurrentCollectionPhase(phaseId);
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
    return currentTrace;
  }
}
