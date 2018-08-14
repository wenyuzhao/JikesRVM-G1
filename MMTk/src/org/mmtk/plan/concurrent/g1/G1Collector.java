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
  //protected final Validation validationTrace = new Validation();
  protected TraceLocal currentTrace;
  protected RemSet.Builder remSetBuilder = new RemSet.Builder(markTrace, G1.regionSpace);
  protected RegionSpace.RegionIterator regionIterator = G1.regionSpace.new RegionIterator();

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
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(bytes <= Plan.MAX_NON_LOS_COPY_BYTES);
//      VM.assertions._assert(allocator == PureG1.ALLOC_MC);
//      VM.assertions._assert(ForwardingWord.stateIsBeingForwarded(VM.objectModel.readAvailableBitsWord(original)));
//    }

    Address addr = copy.alloc(bytes, align, offset);
    //org.mmtk.utility.Memory.assertIsZeroed(addr, bytes);
//    if (VM.VERIFY_ASSERTIONS) {
//      Address region = Region.of(addr);
//      if (!region.isZero()) {
//        VM.assertions._assert(Region.allocated(region));
//        VM.assertions._assert(!Region.relocationRequired(region));
//        VM.assertions._assert(Region.usedSize(region) == 0);
//      } else {
//        Log.writeln("ALLOCATED A NULL REGION");
//      }
//    }
    return addr;
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(allocator == PureG1.ALLOC_DEFAULT);

    //VM.assertions._assert(Region.of(object).NE(EmbeddedMetaData.getMetaDataBase(VM.objectModel.objectStartRef(object))));
    Region.Card.updateCardMeta(object);
    G1.regionSpace.postCopy(object, bytes);

    //if (VM.VERIFY_ASSERTIONS) {
    //  VM.assertions._assert(getCurrentTrace().isLive(object));
      /*if (!getCurrentTrace().willNotMoveInCurrentCollection(object)) {
        Log.write("Block ", Region.of(VM.objectModel.objectStartRef(object)));
        Log.write(" is marked for relocate:");
        Log.writeln(Region.relocationRequired(Region.of(VM.objectModel.objectStartRef(object))) ? "true" : "false");
      }

      VM.assertions._assert(getCurrentTrace().willNotMoveInCurrentCollection(object));*/
    //}
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
//    if (VM.VERIFY_ASSERTIONS) Log.writeln(Phase.getName(phaseId));
    if (phaseId == G1.PREPARE) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!global().markTrace.hasWork());
      currentTrace = markTrace;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      //ConcurrentRemSetRefinement.cardBufPool.prepareNonBlocking();
      return;
    }

    if (phaseId == G1.CLOSURE) {
      G1.currentGCKind = G1.FULL_GC;
      markTrace.completeTrace();
      return;
    }

    if (phaseId == G1.RELEASE) {
      VM.assertions.fail("");
      markTrace.completeTrace();
      markTrace.release();
      //markTrace.release();
      //super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.FINALIZABLE) {
      redirectTrace.traceFinalizables = true;
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.EAGER_CLEANUP_BLOCKS) {
//      if (rendezvous() == 0) {
        //ConcurrentRemSetRefinement.lock.acquire();
//      }
//      rendezvous();
//      long start = VM.statistics.nanoTime();
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
//        long delta = VM.statistics.nanoTime() - start;
//        Log.write("releaseZeroRegions: ");
//        Log.write(VM.statistics.nanosToMillis(delta));
//        Log.writeln(" ms");
      }
//      if (rendezvous() == 0) {
        //ConcurrentRemSetRefinement.lock.release();
//      }
      rendezvous();
      return;
    }

    if (phaseId == G1.CONSTRUCT_REMEMBERED_SETS) {
      Address region;
      while (!(region = G1.regionSpace.regionIterator.next()).isZero()) {
        if (!Region.relocationRequired(region))
          remSetBuilder.scanRegionForConstructingRemSets(region);
      }
      return;
    }

    if (phaseId == G1.REDIRECT_PREPARE) {
      markTrace.completeTrace();
      // ConcurrentRemSetRefinement.lock is already locked

      ConcurrentRemSetRefinement.refineAllDirtyCards();
//      ConcurrentRemSetRefinement.refineAll();
//      rendezvous();
//      ConcurrentRemSetRefinement.refineHotCards();

//      if (rendezvous() == 0) {
//        ConcurrentRemSetRefinement.finishCollectorRefinements();
//        //ConcurrentRemSetRefinement.lock.release();
//      }
//      rendezvous();

      currentTrace = redirectTrace;
      //redirectTrace.log = true;
      redirectTrace.prepare();
      copy.reset();
      //super.collectionPhase(PureG1.PREPARE, primary);
      return;
    }

    if (phaseId == G1.REMEMBERED_SETS) {
//      Region.Card.LOG = true;
      Region.Card.DISABLE_DYNAMIC_HASH_OFFSET = true;
      ConcurrentRemSetRefinement.refineAllDirtyCards();
      Region.Card.DISABLE_DYNAMIC_HASH_OFFSET = false;
//      Region.Card.LOG = false;
//      ConcurrentRemSetRefinement.refineAll();
//      rendezvous();
//      ConcurrentRemSetRefinement.refineHotCards();

//      if (rendezvous() == 0) {
//        ConcurrentRemSetRefinement.finishCollectorRefinements();
//      }
//      rendezvous();
      redirectTrace.processRemSets();
      return;
    }

    if (phaseId == G1.REDIRECT_CLOSURE) {
      redirectTrace.completeTrace();
      redirectTrace.traceFinalizables = false;
      //redirectTrace.remSetsProcessing = false;
      //redirectTrace.processRoots();
      return;
    }

    if (phaseId == G1.REDIRECT_RELEASE) {
      CONCURRENT_CLEANUP_TRIGGERED = false;
      markTrace.release();
      redirectTrace.completeTrace();
      redirectTrace.release();
      copy.reset();
      //ConcurrentRemSetRefinement.cardBufPool.reset();
      super.collectionPhase(G1.RELEASE, primary);
      //if (rendezvous() == 0) RemSet.assertNoPointersToCSet(PureG1.regionSpace, PureG1.relocationSet);
      //rendezvous();
      return;
    }

    if (phaseId == G1.CLEAR_CARD_META) {
      Region.Card.clearCardMetaForUnmarkedCards(G1.regionSpace, false);
      return;
    }

    if (phaseId == G1.CLEANUP_BLOCKS) {
      /*if (rendezvous() == 0) {
        VM.activePlan.resetMutatorIterator();
        PureG1Mutator m;
        while ((m = (PureG1Mutator) VM.activePlan.getNextMutator()) != null) {
          m.enqueueCurrentRSBuffer(false);
        }
        ConcurrentRemSetRefinement.refineAll();
        ConcurrentRemSetRefinement.refineLock.acquire();
        ConcurrentRemSetRefinement.refineLock.release();
        CardTable.assertAllCardsAreNotMarked();
      }*/
      //VM.assertions._assert(!ConcurrentRemSetRefinement.inProgress());
      //VM.assertions._assert(!ConcurrentRemSetRefinement.hasWork());
      //rendezvous();
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(PureG1.relocationSet != null);
//      if (CONCURRENT_CLEANUP_TRIGGERED) return;


      RemSet.cleanupRemSetRefsToRelocationSet(G1.regionSpace, G1.relocationSet, false);
      rendezvous();
      //RemSet.releaseRemSetsOfRelocationSet(PureG1.relocationSet, false);
      G1.regionSpace.cleanupBlocks(G1.relocationSet, false);
      rendezvous();

      //Region.Card.clearCardMetaForUnmarkedCards(PureG1.regionSpace, false);

      //if (rendezvous() == 0) {
      //  VM.activePlan.resetMutatorIterator();
      //  PureG1Mutator m;
      //  while ((m = (PureG1Mutator) VM.activePlan.getNextMutator()) != null) {
      //    VM.assertions._assert(m.rsBufferIsEmpty());
          //Log.writeln("Mutator #", m.getId());
      //  }
        //CardTable.assertAllCardsAreNotMarked();
        //VM.assertions._assert(!ConcurrentRemSetRefinement.inProgress());
        //VM.assertions._assert(!ConcurrentRemSetRefinement.hasWork());
      //}
      //rendezvous();
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

  static boolean CONCURRENT_CLEANUP_TRIGGERED = false;
//  static

  @Override
  @Unpreemptible
  public void concurrentCollectionPhase(short phaseId) {
    //lock.acquire();
//    if (VM.VERIFY_ASSERTIONS) Log.writeln(Phase.getName(phaseId), getId());

    if (phaseId == G1.CONCURRENT_CONSTRUCT_REMEMBERED_SETS) {
      Address region;
      while (!(region = G1.regionSpace.regionIterator.next()).isZero()) {
        if (!Region.relocationRequired(region))
          remSetBuilder.scanRegionForConstructingRemSets(region);
        if (group.isAborted()) {
          break;
        }
      }
      if (rendezvous() == 0) {
        continueCollecting = false;
        if (!group.isAborted()) {
          /* We are responsible for ensuring termination. */
          if (Options.verbose.getValue() >= 2) Log.writeln("< requesting mutator flush >");
          VM.collection.requestMutatorFlush();

          if (Options.verbose.getValue() >= 2) Log.writeln("< mutators flushed >");

          if (concurrentTraceComplete()) {
            continueCollecting = Phase.notifyConcurrentPhaseComplete();
          } else {
            continueCollecting = true;
            Phase.notifyConcurrentPhaseIncomplete();
          }
        }
      }
      rendezvous();
      return;
    }

    if (phaseId == G1.CONCURRENT_CLEANUP) {
      CONCURRENT_CLEANUP_TRIGGERED = true;
      RemSet.cleanupRemSetRefsToRelocationSet(G1.regionSpace, G1.relocationSet, true);
      rendezvous();
      G1.regionSpace.cleanupBlocks(G1.relocationSet, true);
      //rendezvous();
      //Region.Card.clearCardMetaForUnmarkedCards(PureG1.regionSpace, true);
      if (rendezvous() == 0) {
        continueCollecting = false;
        if (!group.isAborted()) {
          VM.collection.requestMutatorFlush();
          if (concurrentTraceComplete()) {
            continueCollecting = Phase.notifyConcurrentPhaseComplete();
          } else {
            continueCollecting = true;
            Phase.notifyConcurrentPhaseIncomplete();
          }
        }
      }
      rendezvous();
      return;
    }

    if (phaseId == G1.CONCURRENT_CLOSURE) {
      currentTrace = markTrace;
    }

    super.concurrentCollectionPhase(phaseId);
    //lock.release();
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
