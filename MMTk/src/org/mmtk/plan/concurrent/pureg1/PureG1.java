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
import org.mmtk.policy.*;
import org.mmtk.utility.Log;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.options.*;
import org.mmtk.utility.sanitychecker.SanityChecker;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements a simple semi-space collector. See the Jones
 * &amp; Lins GC book, section 2.2 for an overview of the basic
 * algorithm. This implementation also includes a large object space
 * (LOS), and an uncollected "immortal" space.<p>
 *
 * All plans make a clear distinction between <i>global</i> and
 * <i>thread-local</i> activities.  Global activities must be
 * synchronized, whereas no synchronization is required for
 * thread-local activities.  Instances of Plan map 1:1 to "kernel
 * threads" (aka CPUs).  Thus instance
 * methods allow fast, unsychronized access to Plan utilities such as
 * allocation and collection.  Each instance rests on static resources
 * (such as memory and virtual memory resources) which are "global"
 * and therefore "static" members of Plan.  This mapping of threads to
 * instances is crucial to understanding the correctness and
 * performance properties of this plan.
 */
@Uninterruptible
public class PureG1 extends Concurrent {

  /****************************************************************************
   *
   * Class variables
   */

  /** One of the two semi spaces that alternate roles at each collection */
  public static final RegionSpace regionSpace = new RegionSpace("g1", VMRequest.discontiguous());
  public static final int MC = regionSpace.getDescriptor();

  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace redirectTrace = new Trace(metaDataSpace);
  public static AddressArray blocksSnapshot, relocationSet;
  //public static boolean concurrentMarkingInProgress = false;

  static {
    Options.g1ReservePercent = new G1ReservePercent();
    Options.g1InitiatingHeapOccupancyPercent = new G1InitiatingHeapOccupancyPercent();
    Options.g1GCLiveThresholdPercent = new G1GCLiveThresholdPercent();
    Options.g1MaxNewSizePercent = new G1MaxNewSizePercent();
    Options.g1NewSizePercent = new G1NewSizePercent();
    Options.g1HeapWastePercent = new G1HeapWastePercent();
    Region.Card.enable();
    regionSpace.makeAllocAsMarked();
    smallCodeSpace.makeAllocAsMarked();
    nonMovingSpace.makeAllocAsMarked();
  }
  static boolean log = false;
  /**
   *
   */
  public static final int ALLOC_MC = Plan.ALLOC_DEFAULT;
  public static final int SCAN_MARK = 0;
  public static final int SCAN_REDIRECT = 1;

  /* Phases */
  public static final short REDIRECT_PREPARE = Phase.createSimple("redirect-prepare");
  public static final short REDIRECT_CLOSURE = Phase.createSimple("redirect-closure");
  public static final short REDIRECT_RELEASE = Phase.createSimple("redirect-release");
  //public static final short RELOCATE_UPDATE_POINTERS = Phase.createSimple("relocate-update-pointers");

  public static final short RELOCATION_SET_SELECTION_PREPARE = Phase.createSimple("relocation-set-selection-prepare");
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection");

  public static final short relocationSetSelection = Phase.createComplex("relocationSetSelection",
    Phase.scheduleGlobal(RELOCATION_SET_SELECTION_PREPARE),
    Phase.scheduleMutator(RELOCATION_SET_SELECTION_PREPARE),
    Phase.scheduleGlobal(RELOCATION_SET_SELECTION)
  );
  //public static final short EVACUATION = Phase.createSimple("evacuation");
  //public static final short PREPARE_EVACUATION = Phase.createSimple("prepare-evacuation");
  public static final short CLEAR_CARD_META = Phase.createSimple("clear-card-meta");
  public static final short CLEANUP_BLOCKS = Phase.createSimple("cleanup-blocks");
  public static final short EAGER_CLEANUP_BLOCKS = Phase.createSimple("eager-cleanup-blocks");
  public static final short REMEMBERED_SETS = Phase.createSimple("remembered-sets");
  public static final short CONSTRUCT_REMEMBERED_SETS = Phase.createSimple("construct-remembered-sets");

  protected static final short preemptConcurrentCleanup = Phase.createComplex("preeempt-concurrent-cleanup", null,
      Phase.scheduleCollector(CLEANUP_BLOCKS));

  public static final short CONCURRENT_CLEANUP = Phase.createConcurrent("concurrent-cleanup",
      Phase.scheduleComplex(preemptConcurrentCleanup));

  public static final short CONCURRENT_CONSTRUCT_REMEMBERED_SETS = Phase.createConcurrent("concurrent-construct-remembered-sets",
      Phase.scheduleCollector(CONSTRUCT_REMEMBERED_SETS));

  //public static final short CONCURRENT_LOCK_MUTATORS = Phase.createConcurrent("flush-refinement-thread", Phase.scheduleMutator(FLUSH_MUTATOR));
  /*protected static final short concurrentLockMutators = Phase.createComplex("concurrent-lock-mutators", null,
      Phase.scheduleGlobal    (SET_BARRIER_ACTIVE),
      Phase.scheduleMutator   (SET_BARRIER_ACTIVE),
      Phase.scheduleCollector (FLUSH_COLLECTOR),
      Phase.scheduleConcurrent(CONCURRENT_LOCK_MUTATORS),
      Phase.scheduleGlobal    (CLEAR_BARRIER_ACTIVE),
      Phase.scheduleMutator   (CLEAR_BARRIER_ACTIVE));*/
  //public static final short MARK_RELEASE = Phase.createSimple("mark-release");
  public static final boolean GENERATIONAL = true;
  public static final short YOUNG_GC = 1;
  public static final short MIXED_GC = 2;
  public static final short FULL_GC = 3;
  public static short currentGCKind = 0;

  public static final short relocationPhase = Phase.createComplex("relocation", null,
    Phase.scheduleMutator  (REDIRECT_PREPARE),
    Phase.scheduleGlobal   (REDIRECT_PREPARE),
    Phase.scheduleCollector(REDIRECT_PREPARE),


//      Phase.scheduleMutator(CONSTRUCT_REMEMBERED_SETS),
      Phase.scheduleCollector(FINALIZABLE),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),
      Phase.scheduleCollector(EAGER_CLEANUP_BLOCKS),
//      Phase.scheduleCollector(FINALIZABLE),
//      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
////      Phase.scheduleCollector(REDIRECT_CLOSURE),
//      Phase.scheduleCollector(EAGER_CLEANUP_BLOCKS),
//      Phase.scheduleGlobal(CONSTRUCT_REMEMBERED_SETS),
//      Phase.scheduleConcurrent(CONCURRENT_CONSTRUCT_REMEMBERED_SETS),



      /*Phase.scheduleMutator  (REMEMBERED_SETS),
      Phase.scheduleGlobal   (REMEMBERED_SETS),
      Phase.scheduleCollector(REMEMBERED_SETS),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),*/

    //Phase.scheduleComplex  (prepareStacks),
    Phase.scheduleCollector(STACK_ROOTS),
    Phase.scheduleGlobal   (STACK_ROOTS),
    Phase.scheduleCollector(ROOTS),
    Phase.scheduleGlobal   (ROOTS),
      /*Phase.scheduleMutator  (REMEMBERED_SETS),
      Phase.scheduleGlobal   (REMEMBERED_SETS),
      Phase.scheduleCollector(REMEMBERED_SETS),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),*/
//    Phase.scheduleGlobal   (REDIRECT_CLOSURE),
//    Phase.scheduleCollector(REDIRECT_CLOSURE),
    Phase.scheduleCollector(SOFT_REFS),
//    Phase.scheduleGlobal   (REDIRECT_CLOSURE),
//    Phase.scheduleCollector(REDIRECT_CLOSURE),
    Phase.scheduleCollector(WEAK_REFS),
//    Phase.scheduleCollector(FINALIZABLE),
    Phase.scheduleGlobal   (REDIRECT_CLOSURE),
    Phase.scheduleCollector(REDIRECT_CLOSURE),
    Phase.scheduleCollector(PHANTOM_REFS),


    //Phase.scheduleGlobal   (REDIRECT_CLOSURE),
    //Phase.scheduleCollector(REDIRECT_CLOSURE),
    Phase.scheduleComplex  (forwardPhase),

      Phase.scheduleMutator  (REMEMBERED_SETS),
      Phase.scheduleGlobal   (REMEMBERED_SETS),
      Phase.scheduleCollector(REMEMBERED_SETS),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),

    //Phase.scheduleGlobal   (REDIRECT_CLOSURE),

      Phase.scheduleCollector(CLEANUP_BLOCKS),
      Phase.scheduleCollector(CLEAR_CARD_META),
      //Phase.scheduleConcurrent(CONCURRENT_CLEANUP),


    Phase.scheduleMutator  (REDIRECT_RELEASE),
    Phase.scheduleCollector(REDIRECT_RELEASE),
    Phase.scheduleGlobal   (REDIRECT_RELEASE)



    //Phase.scheduleComplex(Validation.validationPhase)
  );




  public short _collection = Phase.createComplex("_collection", null,
    Phase.scheduleComplex  (initPhase),
    // Mark
    Phase.scheduleComplex  (rootClosurePhase),

    //Phase.scheduleComplex  (refTypeClosurePhase),
      //Phase.scheduleComplex  (forwardPhase),
      Phase.scheduleCollector  (SOFT_REFS),
      Phase.scheduleGlobal     (CLOSURE),
      Phase.scheduleCollector  (CLOSURE),
      Phase.scheduleCollector(WEAK_REFS),
//      Phase.scheduleCollector(FINALIZABLE),
      Phase.scheduleGlobal   (CLOSURE),
      Phase.scheduleCollector(CLOSURE),
      Phase.scheduleCollector(PHANTOM_REFS),
      Phase.scheduleGlobal(RELEASE),
      //Phase.scheduleGlobal(PAUSE_REFINEMENT_THREADS),
      /*Phase.scheduleCollector  (WEAK_REFS),
      //Phase.scheduleCollector  (FINALIZABLE),
      Phase.scheduleGlobal     (CLOSURE),
      Phase.scheduleCollector  (CLOSURE),
      //Phase.schedulePlaceholder(WEAK_TRACK_REFS),
      Phase.scheduleCollector  (PHANTOM_REFS),
      Phase.scheduleGlobal     (CLOSURE),
      Phase.scheduleCollector  (CLOSURE),*/
    //Phase.scheduleComplex  (completeClosurePhase),
      //Phase.scheduleComplex(concurrentLockMutators),
      //hase.scheduleMutator   (CLEAR_BARRIER_ACTIVE),
    //Phase.scheduleCollector(RELEASE),
    //Phase.scheduleGlobal(RELEASE),
    //Phase.scheduleComplex(completeClosurePhase),

//    Phase.scheduleGlobal(CONSTRUCT_REMEMBERED_SETS),
//    Phase.scheduleConcurrent(CONCURRENT_CONSTRUCT_REMEMBERED_SETS),
    Phase.scheduleComplex  (relocationSetSelection),


    Phase.scheduleComplex  (relocationPhase),

      //Phase.scheduleGlobal(CLEANUP_BLOCKS),


    Phase.scheduleComplex  (finishPhase)
      //Phase.scheduleMutator(COMPLETE)
  );

  /**
   * Constructor
   */
  public PureG1() {
    collection = _collection;
  }
  /****************************************************************************
   *
   * Collection
   */
  static long totalSTWTime = 0;
  static long startTime = 0;
  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId) {
//    if (VM.VERIFY_ASSERTIONS) {
//      Log.write("Global ");
//      Log.write(Phase.getName(phaseId));
//      Log.write(" total ");
//      Log.write(VM.statistics.nanosToMillis( VM.statistics.nanoTime() - startTime));
//      Log.writeln(" ms");
//    }
    if (phaseId == PREPARE) {
      super.collectionPhase(phaseId);
      markTrace.prepareNonBlocking();
      regionSpace.prepare(true);
      return;
    }

    if (phaseId == CLOSURE) {
      return;
    }

    if (phaseId == RELEASE) {
      //PureG1.stacksPrepared = false;
      if (currentGCKind != 0) {
        // FULL_GC
//        if (VM.VERIFY_ASSERTIONS) Log.writeln("[G1: FULL_GC]");
      } else if (!GENERATIONAL || regionSpace.heapWastePercent(true) >= Options.g1HeapWastePercent.getValue()) {
        currentGCKind = MIXED_GC;
//        if (VM.VERIFY_ASSERTIONS) Log.writeln("[G1: MIXED_GC]");
      } else {
        currentGCKind = YOUNG_GC;
//        if (VM.VERIFY_ASSERTIONS) Log.writeln("[G1: YOUNG_GC]");
      }

      PauseTimePredictor.stopTheWorldStart();
      startTime = VM.statistics.nanoTime();
      ConcurrentRemSetRefinement.pause();
      ConcurrentRemSetRefinement.relocationSetOnly = true;
      //VM.assertions._assert(false);
      //startTime = VM.statistics.nanoTime();
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!markTrace.hasWork());
      //markTrace.release();

      //stacksPrepared = false;
      //inConcurrentCollection = false;
      //regionSpace.release();
      //super.collectionPhase(phaseId);
      //inConcurrentCollection = false;
      //VM.assertions._assert(!stacksPrepared);
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION_PREPARE) {
      blocksSnapshot = regionSpace.snapshotBlocks(currentGCKind == YOUNG_GC);
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION) {
      //blocksSnapshot = regionSpace.snapshotBlocks();
      relocationSet = RegionSpace.computeRelocationBlocks(blocksSnapshot, currentGCKind != YOUNG_GC, false);
      if (currentGCKind == YOUNG_GC || currentGCKind == MIXED_GC) {
        PauseTimePredictor.predict(relocationSet, currentGCKind);
      }
      RegionSpace.markRegionsAsRelocate(relocationSet);
      blocksSnapshot = null;
      return;
    }

    if (phaseId == CONSTRUCT_REMEMBERED_SETS) {
      regionSpace.regionIterator.reset();
      return;
    }

    if (phaseId == REDIRECT_PREPARE) {



      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!stacksPrepared);
      //stacksPrepared = false;
      ConcurrentRemSetRefinement.resume();
      VM.activePlan.resetMutatorIterator();
      PureG1Mutator m;
      while ((m = (PureG1Mutator) VM.activePlan.getNextMutator()) != null) {
        m.dropCurrentRSBuffer();
//        m.enqueueCurrentRSBuffer(true);
      }
      ConcurrentRemSetRefinement.pause();
      //ConcurrentRemSetRefinement.lock.acquire();
      //ConcurrentRemSetRefinement.refineAll();
      //CardTable.assertAllCardsAreNotMarked();

      //super.collectionPhase(PREPARE);
      redirectTrace.prepare();
      //regionSpace.prepare(false);
      return;
    }

    if (phaseId == REMEMBERED_SETS) {

      VM.activePlan.resetMutatorIterator();
      PureG1Mutator m;
      while ((m = (PureG1Mutator) VM.activePlan.getNextMutator()) != null) {
        m.dropCurrentRSBuffer();
//        m.enqueueCurrentRSBuffer(false);
      }
      //ConcurrentRemSetRefinement.lock.acquire();
      //ConcurrentRemSetRefinement.refineAll();
      //RemSet.noCSet = true;
      //super.collectionPhase(PREPARE);
      //ConcurrentRemSetRefinement.refineAll();
      //if (VM.VERIFY_ASSERTIONS) CardTable.assertAllCardsAreNotMarked();
      //regionSpace.prepare();
      return;
    }

    if (phaseId == REDIRECT_CLOSURE) {
      return;
    }

    if (phaseId == REDIRECT_RELEASE) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!markTrace.hasWork());
      regionSpace.promoteAllRegionsAsOldGeneration();
      markTrace.release();
      redirectTrace.release();
      regionSpace.release();
      super.collectionPhase(RELEASE);
      //ConcurrentRemSetRefinement.cardBufPool.clearDeque(1);
      return;
    }

    if (phaseId == COMPLETE) {
      ConcurrentRemSetRefinement.relocationSetOnly = false;
      ConcurrentRemSetRefinement.resume();
      PauseTimePredictor.stopTheWorldEnd();
      super.collectionPhase(COMPLETE);
      currentGCKind = NOT_IN_GC;
      return;
    }

    super.collectionPhase(phaseId);
  }

  /****************************************************************************
   *
   * Accounting
   */

  @Override
  protected boolean collectionRequired(boolean spaceFull, Space space) {
    int usedPages = getPagesUsed() - metaDataSpace.reservedPages();
    int totalPages = getTotalPages() - metaDataSpace.reservedPages();
    if ((totalPages - usedPages) < (totalPages * Options.g1ReservePercent.getValue() / 100)) {
      return true;
    }
    return super.collectionRequired(spaceFull, space);
  }

  @Override
  protected boolean concurrentCollectionRequired() {
//    return false;
    int usedPages = getPagesUsed() - metaDataSpace.reservedPages();
    int totalPages = getTotalPages() - metaDataSpace.reservedPages();
    return !Phase.concurrentPhaseActive() && ((usedPages * 100) > (totalPages * Options.g1InitiatingHeapOccupancyPercent.getValue()));
  }

  /**
   * Return the number of pages reserved for copying.
   */
  @Override
  public final int getCollectionReserve() {
    // we must account for the number of pages required for copying,
    // which equals the number of semi-space pages reserved
    return regionSpace.getCollectionReserve() + super.getCollectionReserve(); // TODO: Fix this
  }

  @Override
  @Interruptible
  protected void spawnCollectorThreads(int numThreads) {
    super.spawnCollectorThreads(numThreads);

    //ConcurrentRemSetRefinement.NUM_WORKERS = numThreads;
    int refineThreads = numThreads;// <= 2 ? 1 : numThreads >> 1;
    for (int i = 0; i < refineThreads; i++) {
      VM.collection.spawnCollectorContext(new ConcurrentRemSetRefinement(refineThreads));
    }

//    Region.dumpMeta();
  }

  @Override
  public int sanityExpectedRC(ObjectReference object, int sanityRootRC) {
    Space space = Space.getSpaceForObject(object);
    // Nursery
    if (space == regionSpace) {
      // We are never sure about objects in MC.
      // This is not very satisfying but allows us to use the sanity checker to
      // detect dangling pointers.
      return SanityChecker.UNSURE;
    }
    return super.sanityExpectedRC(object, sanityRootRC);
  }

  /**
   * Return the number of pages reserved for use given the pending
   * allocation.  This is <i>exclusive of</i> space reserved for
   * copying.
   */
  @Override
  public int getPagesUsed() {
    return super.getPagesUsed() + regionSpace.reservedPages();
  }

  /**
   * Return the number of pages available for allocation, <i>assuming
   * all future allocation is to the semi-space</i>.
   *
   * @return The number of pages available for allocation, <i>assuming
   * all future allocation is to the semi-space</i>.
   */
  // @Override public final int getPagesAvail() { return(super.getPagesAvail()) >> 1; }

  @Override
  public boolean willNeverMove(ObjectReference object) {
    if (Space.isInSpace(MC, object)) return false;
    return super.willNeverMove(object);
  }

  @Override
  @Interruptible
  protected void registerSpecializedMethods() {
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, PureG1MarkTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_REDIRECT, PureG1RedirectTraceLocal.class);
    super.registerSpecializedMethods();
  }
}
