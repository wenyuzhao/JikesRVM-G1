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
package org.mmtk.plan.concurrent.pureg1;

import org.mmtk.plan.*;
import org.mmtk.plan.concurrent.Concurrent;
import org.mmtk.plan.concurrent.ConcurrentCollector;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.RemSet;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Pure;
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
 * See {@link PureG1} for an overview of the semi-space algorithm.
 *
 * @see PureG1
 * @see PureG1Mutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class PureG1Collector extends ConcurrentCollector {

  /****************************************************************************
   * Instance fields
   */

  /**
   *
   */
  protected final RegionAllocator copy = new RegionAllocator(PureG1.regionSpace, true);
  protected final PureG1MarkTraceLocal markTrace = new PureG1MarkTraceLocal(global().markTrace);
  protected final PureG1RedirectTraceLocal redirectTrace = new PureG1RedirectTraceLocal(global().redirectTrace);
  //protected final Validation validationTrace = new Validation();
  protected TraceLocal currentTrace;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public PureG1Collector() {}

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
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(bytes <= Plan.MAX_NON_LOS_COPY_BYTES);
      VM.assertions._assert(allocator == PureG1.ALLOC_MC);
      VM.assertions._assert(ForwardingWord.stateIsBeingForwarded(VM.objectModel.readAvailableBitsWord(original)));
    }

    Address addr = copy.alloc(bytes, align, offset);
    //org.mmtk.utility.Memory.assertIsZeroed(addr, bytes);
    if (VM.VERIFY_ASSERTIONS) {
      Address region = Region.of(addr);
      if (!region.isZero()) {
        VM.assertions._assert(Region.allocated(region));
        VM.assertions._assert(!Region.relocationRequired(region));
        VM.assertions._assert(Region.usedSize(region) == 0);
      } else {
        Log.writeln("ALLOCATED A NULL REGION");
      }
    }
    return addr;
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(allocator == PureG1.ALLOC_DEFAULT);

    VM.assertions._assert(Region.of(object).NE(EmbeddedMetaData.getMetaDataBase(VM.objectModel.objectStartRef(object))));
    Region.Card.updateCardMeta(object);
    PureG1.regionSpace.postCopy(object, bytes);

    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(getCurrentTrace().isLive(object));
      /*if (!getCurrentTrace().willNotMoveInCurrentCollection(object)) {
        Log.write("Block ", Region.of(VM.objectModel.objectStartRef(object)));
        Log.write(" is marked for relocate:");
        Log.writeln(Region.relocationRequired(Region.of(VM.objectModel.objectStartRef(object))) ? "true" : "false");
      }

      VM.assertions._assert(getCurrentTrace().willNotMoveInCurrentCollection(object));*/
    }
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
    if (phaseId == PureG1.PREPARE) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!global().markTrace.hasWork());
      currentTrace = markTrace;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      //ConcurrentRemSetRefinement.cardBufPool.prepareNonBlocking();
      return;
    }

    if (phaseId == PureG1.CLOSURE) {
      PureG1.currentGCKind = PureG1.FULL_GC;
      markTrace.completeTrace();
      return;
    }

    if (phaseId == PureG1.RELEASE) {
      VM.assertions.fail("");
      markTrace.completeTrace();
      markTrace.release();
      //markTrace.release();
      //super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == PureG1.REDIRECT_PREPARE) {
      markTrace.completeTrace();
      // ConcurrentRemSetRefinement.lock is already locked

      ConcurrentRemSetRefinement.refineAll();
      rendezvous();
      ConcurrentRemSetRefinement.refineHotCards();

      if (rendezvous() == 0) {
        ConcurrentRemSetRefinement.finishCollectorRefinements();
        ConcurrentRemSetRefinement.lock.release();
      }
      rendezvous();

      currentTrace = redirectTrace;
      //redirectTrace.log = true;
      redirectTrace.prepare();
      copy.reset();
      //super.collectionPhase(PureG1.PREPARE, primary);
      return;
    }

    if (phaseId == PureG1.REMEMBERED_SETS) {

      ConcurrentRemSetRefinement.refineAll();
      rendezvous();
      ConcurrentRemSetRefinement.refineHotCards();

      if (rendezvous() == 0) {
        ConcurrentRemSetRefinement.finishCollectorRefinements();
        ConcurrentRemSetRefinement.lock.release();
      }
      rendezvous();
      /*if (rendezvous() == 0) {
        ConcurrentRemSetRefinement.FilledRSBufferQueue.lock.acquire();
      }
      rendezvous();
      ConcurrentRemSetRefinement.refineAll();
      if (rendezvous() == 0) {
        ConcurrentRemSetRefinement.HotCardsQueue.lock.acquire();
      }
      rendezvous();
      ConcurrentRemSetRefinement.refineHotCards();
      if (rendezvous() == 0) {
        ConcurrentRemSetRefinement.finishCollectorRefinements();
        ConcurrentRemSetRefinement.FilledRSBufferQueue.lock.release();
        ConcurrentRemSetRefinement.HotCardsQueue.lock.release();
      }
      rendezvous();
      //redirectTrace.processor.unionRemSets(PureG1.regionSpace, PureG1.relocationSet, false);
      rendezvous();*/
      //redirectTrace.remSetsProcessing = true;
      redirectTrace.processRemSets();
      return;
    }

    if (phaseId == PureG1.REDIRECT_CLOSURE) {
      redirectTrace.completeTrace();
      rendezvous();
      //redirectTrace.remSetsProcessing = false;
      //redirectTrace.processRoots();
      return;
    }

    if (phaseId == PureG1.REDIRECT_RELEASE) {
      markTrace.release();
      redirectTrace.completeTrace();
      redirectTrace.release();
      copy.reset();
      //ConcurrentRemSetRefinement.cardBufPool.reset();
      super.collectionPhase(PureG1.RELEASE, primary);
      //if (rendezvous() == 0) RemSet.assertNoPointersToCSet(PureG1.regionSpace, PureG1.relocationSet);
      //rendezvous();
      Region.Card.clearCardMetaForUnmarkedCards(PureG1.regionSpace, false);
      return;
    }

    if (phaseId == PureG1.CLEANUP_BLOCKS) {
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
      rendezvous();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(PureG1.relocationSet != null);
      RemSet.cleanupRemSetRefsToRelocationSet(PureG1.regionSpace, PureG1.relocationSet, false);
      rendezvous();
      //RemSet.releaseRemSetsOfRelocationSet(PureG1.relocationSet, false);
      PureG1.regionSpace.cleanupBlocks(PureG1.relocationSet, false);
      rendezvous();
      if (rendezvous() == 0) {
        VM.activePlan.resetMutatorIterator();
        PureG1Mutator m;
        while ((m = (PureG1Mutator) VM.activePlan.getNextMutator()) != null) {
          VM.assertions._assert(m.rsBufferIsEmpty());
          //Log.writeln("Mutator #", m.getId());
        }
        //CardTable.assertAllCardsAreNotMarked();
        //VM.assertions._assert(!ConcurrentRemSetRefinement.inProgress());
        //VM.assertions._assert(!ConcurrentRemSetRefinement.hasWork());
      }
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

  @Override
  @Unpreemptible
  public void concurrentCollectionPhase(short phaseId) {
    //lock.acquire();
    if (VM.VERIFY_ASSERTIONS) Log.writeln(Phase.getName(phaseId), getId());

    if (phaseId == PureG1.CONCURRENT_CLOSURE) {
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
  private static PureG1 global() {
    return (PureG1) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
