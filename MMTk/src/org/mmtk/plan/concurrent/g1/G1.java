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
import org.mmtk.plan.concurrent.Concurrent;
import org.mmtk.policy.*;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.options.*;
import org.mmtk.utility.sanitychecker.SanityChecker;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
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
public class G1 extends Concurrent {

  /****************************************************************************
   *
   * Class variables
   */

  public static final RegionSpace regionSpace = new RegionSpace("g1", VMRequest.discontiguous());
  public static final int G1 = regionSpace.getDescriptor();

  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace nurseryTrace = new Trace(metaDataSpace);
  public final Trace matureTrace = new Trace(metaDataSpace);
  public final Trace validateTrace = new Trace(metaDataSpace);
  public static AddressArray relocationSet;

  static {
    Options.g1GenerationalMode = new G1GenerationalMode();
    Options.g1ReservePercent = new G1ReservePercent();
    Options.g1InitiatingHeapOccupancyPercent = new G1InitiatingHeapOccupancyPercent();
    Options.g1GCLiveThresholdPercent = new G1GCLiveThresholdPercent();
    Options.g1MaxNewSizePercent = new G1MaxNewSizePercent();
    Options.g1NewSizePercent = new G1NewSizePercent();
    Options.g1HeapWastePercent = new G1HeapWastePercent();
    Region.USE_CARDS = true;
    regionSpace.makeAllocAsMarked();
    smallCodeSpace.makeAllocAsMarked();
    nonMovingSpace.makeAllocAsMarked();
  }

  /** Allocators */
  public static final int ALLOC_EDEN = Plan.ALLOC_DEFAULT;
  public static final int ALLOC_SURVIVOR = ALLOCATORS + 1;
  public static final int ALLOC_OLD = ALLOCATORS + 2;

  /** Specialized scans */
  public static final int SCAN_NURSERY = 0;
  public static final int SCAN_MARK = 1;
  public static final int SCAN_MATURE = 2;

  /** GC kinds & options */
  public static final short YOUNG_GC = 1;
  public static final short MIXED_GC = 2;
  public static final short FULL_GC = 3;
  public static short currentGCKind = NOT_IN_GC, lastGCKind = NOT_IN_GC;

  /* Phases */
  public static final short EAGER_CLEANUP = Phase.createSimple("eager-cleanup");
  public static final short preemptConcurrentEagerCleanup = Phase.createComplex("preempt-concurrent-eager-cleanup", null,
      Phase.scheduleCollector(EAGER_CLEANUP));
  public static final short CONCURRENT_EAGER_CLEANUP = Phase.createConcurrent("concurrent-eager-cleanup",
      Phase.scheduleComplex(preemptConcurrentEagerCleanup));
  public static final short EVACUATE = Phase.createSimple("evacuate");
  public static final short REFINE_CARDS = Phase.createSimple("refine-cards");
  public static final short REMEMBERED_SETS = Phase.createSimple("remembered-sets");
  public static final short FORWARD_PREPARE = Phase.createSimple("forward-prepare");
  public static final short FORWARD_CLOSURE = Phase.createSimple("forward-closure");
  public static final short FORWARD_RELEASE = Phase.createSimple("forward-release");
  public static final short CLEAR_CARD_META = Phase.createSimple("clear-card-meta");
  // Relocation set selection phases
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection");
  public static final short preemptConcurrentRelocationSetSelection = Phase.createComplex("preempt-relocation-set-selection", null,
      Phase.scheduleCollector(RELOCATION_SET_SELECTION));
  public static final short CONCURRENT_RELOCATION_SET_SELECTION = Phase.createConcurrent("concurrent-relocation-set-selection",
      Phase.scheduleComplex(preemptConcurrentRelocationSetSelection));
  // Cleanup phases
  public static final short CLEANUP = Phase.createSimple("cleanup");
  public static final short preemptConcurrentCleanup = Phase.createComplex("preempt-concurrent-cleanup", null,
      Phase.scheduleCollector(CLEANUP));
  public static final short CONCURRENT_CLEANUP = Phase.createConcurrent("concurrent-cleanup",
      Phase.scheduleComplex(preemptConcurrentCleanup));

  public static final short refineDirtyCards = Phase.createComplex("refine-cards-phase", null,
      Phase.scheduleMutator  (REFINE_CARDS),
      Phase.scheduleGlobal   (REFINE_CARDS),
      Phase.scheduleCollector(REFINE_CARDS)
  );
  protected static final short forwardRootClosurePhase = Phase.createComplex("forward-initial-closure", null,
      Phase.scheduleMutator  (FORWARD_PREPARE),
      Phase.scheduleGlobal   (FORWARD_PREPARE),
      Phase.scheduleCollector(FORWARD_PREPARE),
      Phase.scheduleMutator  (PREPARE_STACKS),
      Phase.scheduleGlobal   (PREPARE_STACKS),
      Phase.scheduleCollector(STACK_ROOTS),
      Phase.scheduleGlobal   (STACK_ROOTS),
      Phase.scheduleCollector(ROOTS),
      Phase.scheduleGlobal   (ROOTS),
      Phase.scheduleCollector(FORWARD_CLOSURE),
      Phase.scheduleMutator  (REFINE_CARDS),
      Phase.scheduleGlobal   (REFINE_CARDS),
      Phase.scheduleCollector(REFINE_CARDS),
      Phase.scheduleCollector(REMEMBERED_SETS),
      Phase.scheduleGlobal   (REMEMBERED_SETS),
      Phase.scheduleCollector(FORWARD_CLOSURE)
  );
  protected static final short forwardRefTypeClosurePhase = Phase.createComplex("forward-refType-closure", null,
      Phase.scheduleCollector(SOFT_REFS),
      Phase.scheduleCollector(FORWARD_CLOSURE),
      Phase.scheduleCollector(WEAK_REFS),
      Phase.scheduleCollector(FINALIZABLE),
      Phase.scheduleCollector(FORWARD_CLOSURE),
      Phase.scheduleCollector(PHANTOM_REFS),
      Phase.scheduleCollector(FORWARD_CLOSURE)
  );
  protected static final short forwardCompleteClosurePhase = Phase.createComplex("forward-release", null,
      Phase.scheduleCollector(CLEANUP),
      Phase.scheduleCollector(CLEAR_CARD_META),
      Phase.scheduleMutator  (FORWARD_RELEASE),
      Phase.scheduleCollector(FORWARD_RELEASE),
      Phase.scheduleGlobal   (FORWARD_RELEASE)
  );

  /** Nursery collection phases */
  public static final short nurseryCollection = Phase.createComplex("nursery-collection", null,
      // Init
      Phase.scheduleComplex  (initPhase),
      // Evacuate
      Phase.scheduleCollector(RELOCATION_SET_SELECTION),
      Phase.scheduleMutator  (RELOCATION_SET_SELECTION),
//      Phase.scheduleComplex  (refineDirtyCards),
      Phase.scheduleComplex  (forwardRootClosurePhase),
      Phase.scheduleComplex  (forwardRefTypeClosurePhase),
      Phase.scheduleComplex  (forwardCompleteClosurePhase),

//      Phase.scheduleComplex  (Validator.validationPhase),
//      Phase.scheduleCollector(CLEAR_CARD_META),
      // Complete
      Phase.scheduleComplex  (finishPhase)
  );

  /** Mature collection phases */
  public short matureCollection = Phase.createComplex("mature-collection", null,
      // Init
      Phase.scheduleComplex  (initPhase),
      // Mark
      Phase.scheduleComplex  (rootClosurePhase),
      Phase.scheduleComplex  (refTypeClosurePhase),
      Phase.scheduleComplex  (completeClosurePhase),
      // Evacuate
      Phase.scheduleCollector(RELOCATION_SET_SELECTION),
      Phase.scheduleCollector(EAGER_CLEANUP),
      Phase.scheduleCollector(EVACUATE),
      // Update pointers
//      Phase.scheduleComplex  (refineDirtyCards),
      Phase.scheduleComplex  (forwardRootClosurePhase),
      Phase.scheduleComplex  (forwardRefTypeClosurePhase),
      Phase.scheduleComplex  (forwardCompleteClosurePhase),

//      Phase.scheduleMutator  (REFINE_CARDS),
//      Phase.scheduleGlobal   (REFINE_CARDS),
//      Phase.scheduleCollector(REFINE_CARDS),
//      Phase.scheduleComplex  (Validator.validationPhase),
//      Phase.scheduleCollector(CLEAR_CARD_META),
      // Complete
      Phase.scheduleComplex  (finishPhase)
  );

  /**
   * Constructor
   */
  public G1() {
    collection = matureCollection;
  }

  @Override
  @Interruptible
  public void processOptions() {
    super.processOptions();
    /* Set up the concurrent marking phase */
//    replacePhase(Phase.scheduleCollector(RELOCATION_SET_SELECTION), Phase.scheduleConcurrent(CONCURRENT_RELOCATION_SET_SELECTION));
//    replacePhase(Phase.scheduleCollector(EAGER_CLEANUP), Phase.scheduleConcurrent(CONCURRENT_EAGER_CLEANUP));
//    replacePhase(Phase.scheduleCollector(CLEANUP), Phase.scheduleConcurrent(CONCURRENT_CLEANUP));
  }

  /****************************************************************************
   *
   * Collection
   */
  static long startTime = 0;
//  public static int totalGCs = 0;
//  public static int fullGCs = 0;

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId) {
    if (VM.VERIFY_ASSERTIONS) {
      Log.write("Global ");
      Log.writeln(Phase.getName(phaseId));
    }

    if (phaseId == SET_COLLECTION_KIND) {
      super.collectionPhase(SET_COLLECTION_KIND);
      if (Plan.isUserTriggeredCollection()) {
        currentGCKind = FULL_GC;
      } else if (collection == nurseryCollection) {
        PauseTimePredictor.nurseryGCStart();
        currentGCKind = YOUNG_GC;
      } else {
        currentGCKind = MIXED_GC;
      }
      if (insideHarness) {
        Plan.gcCounts += 1;
      }
      return;
    }

    if (phaseId == INITIATE) {
      super.collectionPhase(INITIATE);
      G1Collector.concurrentRelocationSetSelectionExecuted = false;
      G1Collector.concurrentEagerCleanupExecuted = false;
      G1Collector.concurrentCleanupExecuted = false;
      return;
    }

    if (phaseId == PREPARE) {
      super.collectionPhase(phaseId);
      markTrace.prepareNonBlocking();
      regionSpace.prepare();
      return;
    }

    if (phaseId == CLOSURE) {
      return;
    }

    if (phaseId == RELEASE) {
      if (currentGCKind == MIXED_GC) PauseTimePredictor.stopTheWorldStart();
      startTime = VM.statistics.nanoTime();
      markTrace.release();
      return;
    }

    if (phaseId == REFINE_CARDS) {
      ConcurrentRemSetRefinement.pause();
      return;
    }

    if (phaseId == REMEMBERED_SETS) {
      int remsetPages = 0, remsetCards = 0;
      for (Address region = regionSpace.firstRegion(); !region.isZero(); region = regionSpace.nextRegion(region)) {
        remsetPages += Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).loadInt();
        remsetCards += Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
      }
      remsetLogs[remsetLogCursor] = regionSpace.committedRegions();
      remsetLogs[remsetLogCursor + 1] = remsetPages;
      remsetLogs[remsetLogCursor + 2] = remsetCards;
      remsetLogCursor += 3;
      return;
    }

    if (phaseId == FORWARD_PREPARE) {
      if (nurseryGC()) {
        VM.memory.globalPrepareVMSpace();
//        regionSpace.prepare();
        nurseryTrace.prepare();
      } else {
//        regionSpace.prepareNursery();
        matureTrace.prepare();
      }
      return;
    }

    if (phaseId == FORWARD_RELEASE) {
      (nurseryGC() ? nurseryTrace : matureTrace).release();
      if (!nurseryGC()) {
        super.collectionPhase(RELEASE);
      } else {
        VM.memory.globalReleaseVMSpace();
      }

      return;
    }

    if (phaseId == COMPLETE) {
      ConcurrentRemSetRefinement.resume();
      super.collectionPhase(COMPLETE);
      if (insideHarness && currentGCKind == FULL_GC) {
        Plan.fullGCCounts += 1;
      }

      if (currentGCKind == YOUNG_GC) {
        PauseTimePredictor.nurseryGCEnd();
      } else if (currentGCKind == MIXED_GC) {
        PauseTimePredictor.stopTheWorldEnd();
      }
      lastGCKind = currentGCKind;
      currentGCKind = NOT_IN_GC;
      collection = matureCollection;
      return;
    }

    if (phaseId == Validator.VALIDATE_PREPARE) {
      super.collectionPhase(PREPARE);
      validateTrace.prepare();
      regionSpace.prepare();
      return;
    }

    if (phaseId == Validator.VALIDATE_RELEASE) {
      validateTrace.release();
      regionSpace.release();
      super.collectionPhase(RELEASE);
      return;
    }

    super.collectionPhase(phaseId);
  }

  @Inline
  public boolean nurseryGC() {
    return currentGCKind == YOUNG_GC;
  }

  /****************************************************************************
   *
   * Accounting
   */

  final int TOTAL_LOGICAL_REGIONS = VM.AVAILABLE_END.diff(VM.AVAILABLE_START).toWord().rshl(Region.LOG_BYTES_IN_REGION).toInt();
  final int BOOT_PAGES = VM.AVAILABLE_START.diff(VM.HEAP_START).toInt() / Constants.BYTES_IN_PAGE;
//  final float INIT_HEAP_OCCUPANCY_PERCENT = 1f - Options.g1InitiatingHeapOccupancyPercent.getValue() / 100f;
  float newSizeRatio = Options.g1NewSizePercent.getValue() / 100;

  @Inline
  public final boolean generationalMode() {
    return Options.g1GenerationalMode.getValue();
  }

  final float RESERVE_PERCENT = Options.g1ReservePercent.getValue() / 100f;

  @Override
  @Inline
  protected boolean collectionRequired(boolean spaceFull, Space space) {
//    final float RESERVE_PERCENT = Options.g1ReservePercent.getValue() / 100f;
    // Young GC
    if (generationalMode() && Phase.isPhaseStackEmpty() && (!Plan.gcInProgress()) && (!Phase.concurrentPhaseActive()) && (((float) regionSpace.youngRegions()) > newSizeRatio * ((float) TOTAL_LOGICAL_REGIONS))) {
      collection = nurseryCollection;
      return true;
    }
    // Full GC
    final int totalPages = getTotalPages();
//    boolean fullGCRequired = false;
    final boolean heapFull = ((totalPages - getPagesReserved()) * 10) < totalPages;
    if (spaceFull || heapFull) {
      collection = matureCollection;
      return true;
    }
    return false;

//    int totalPages = getTotalPages();
//    boolean fullGCRequired = false;
//    if (getPagesAvail() - BOOT_PAGES < totalPages * RESERVE_PERCENT) {
//      fullGCRequired = true;
//    }
//    fullGCRequired = fullGCRequired || super.collectionRequired(spaceFull, space);
//    if (fullGCRequired) collection = matureCollection;
//    return fullGCRequired;
  }

//  @Override
//  @Inline
//  protected boolean concurrentCollectionRequired() {
//    int totalPages = getTotalPages();
//    int availPages = getPagesAvail() - BOOT_PAGES;
//    boolean mixedGCRequired = !Phase.concurrentPhaseActive() && (availPages < (totalPages * INIT_HEAP_OCCUPANCY_PERCENT));
//    if (mixedGCRequired) {
////      Log.writeln("Mixed GC Required");
//      collection = matureCollection;
//    }
//    return mixedGCRequired;
//  }

  final float INIT_HEAP_OCCUPANCY_PERCENT = Options.g1InitiatingHeapOccupancyPercent.getValue();
  @Override
  protected boolean concurrentCollectionRequired() {
    boolean mixedGCRequired = !Phase.concurrentPhaseActive() &&
        ((getPagesReserved() * 100) / (getTotalPages())) > INIT_HEAP_OCCUPANCY_PERCENT;
    if (mixedGCRequired) {
      collection = matureCollection;
    }
    return mixedGCRequired;
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

    int refineThreads = 1;//numThreads;// <= 2 ? 1 : numThreads >> 1;
    ConcurrentRemSetRefinement.initialize(refineThreads);
    for (int i = 0; i < refineThreads; i++) {
      VM.collection.spawnCollectorContext(new ConcurrentRemSetRefinement());
    }
  }

  @Override
  public int sanityExpectedRC(ObjectReference object, int sanityRootRC) {
    Space space = Space.getSpaceForObject(object);
    // Nursery
    if (space == regionSpace) {
      // We are never sure about objects in G1.
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
  @Inline
  public int getPagesUsed() {
    return super.getPagesUsed() + regionSpace.reservedPages();
  }

  @Override
  @Inline
  public boolean isCurrentGCNursery() {
    return false;//collection == nurseryCollection;
  }

  @Override
  @Inline
  public boolean lastCollectionFullHeap() {
    return lastGCKind == FULL_GC;
  }

  @Override
  public boolean willNeverMove(ObjectReference object) {
    if (Space.isInSpace(G1, object)) return false;
    return super.willNeverMove(object);
  }

  @Override
  @Interruptible
  protected void registerSpecializedMethods() {
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, G1MarkTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_MATURE, G1MatureTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_NURSERY, G1NurseryTraceLocal.class);
    super.registerSpecializedMethods();
  }
}
