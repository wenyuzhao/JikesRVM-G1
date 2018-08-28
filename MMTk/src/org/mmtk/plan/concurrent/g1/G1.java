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
public class G1 extends Concurrent {

  /****************************************************************************
   *
   * Class variables
   */

  /** One of the two semi spaces that alternate roles at each collection */
  public static final RegionSpace regionSpace = new RegionSpace("g1", VMRequest.discontiguous());
  public static final int G1 = regionSpace.getDescriptor();

  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace nurseryTrace = new Trace(metaDataSpace);
  public final Trace matureTrace = new Trace(metaDataSpace);
  public static AddressArray blocksSnapshot, relocationSet;

  static {
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

  /**
   *
   */
//  public static final int ALLOC_MATURE = Plan.ALLOC_DEFAULT;
  public static final int ALLOC_EDEN = Plan.ALLOC_DEFAULT;
  public static final int ALLOC_SURVIVOR = ALLOCATORS + 1;
  public static final int ALLOC_OLD = ALLOCATORS + 2;
//  public static final int ALLOC_NURSERY = Plan.ALLOC_DEFAULT;
//  public static final int ALLOC_RS = Plan.ALLOC_DEFAULT;
  public static final int SCAN_NURSERY = 0;
  public static final int SCAN_MARK = 1;
  public static final int SCAN_MATURE = 2;

  /* Phases */
  public static final short REDIRECT_PREPARE = Phase.createSimple("redirect-prepare");
  public static final short REDIRECT_CLOSURE = Phase.createSimple("redirect-closure");
  public static final short REDIRECT_RELEASE = Phase.createSimple("redirect-release");

  public static final short RELOCATION_SET_SELECTION_PREPARE = Phase.createSimple("relocation-set-selection-prepare");
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection");

  public static final short relocationSetSelection = Phase.createComplex("relocationSetSelection",
    Phase.scheduleGlobal(RELOCATION_SET_SELECTION_PREPARE),
    Phase.scheduleMutator(RELOCATION_SET_SELECTION_PREPARE),
    Phase.scheduleGlobal(RELOCATION_SET_SELECTION)
  );

  public static final short CLEAR_CARD_META = Phase.createSimple("clear-card-meta");
  public static final short CLEANUP_BLOCKS = Phase.createSimple("cleanup-blocks");
  public static final short REMEMBERED_SETS = Phase.createSimple("remembered-sets");

  public static final boolean GENERATIONAL = true;
  public static final short YOUNG_GC = 1;
  public static final short MIXED_GC = 2;
  public static final short FULL_GC = 3;
  public static short currentGCKind = NOT_IN_GC, lastGCKind = NOT_IN_GC;



  public static final short nurseryCollection = Phase.createComplex("nursery-collection", null,
      Phase.scheduleComplex  (initPhase),

      Phase.scheduleMutator  (REDIRECT_PREPARE),
      Phase.scheduleGlobal   (REDIRECT_PREPARE),
      Phase.scheduleCollector(REDIRECT_PREPARE),

      Phase.scheduleComplex  (relocationSetSelection),
      // rootClosurePhase
      Phase.scheduleMutator  (PREPARE_STACKS),
      Phase.scheduleGlobal   (PREPARE_STACKS),
      Phase.scheduleCollector(STACK_ROOTS),
      Phase.scheduleGlobal   (STACK_ROOTS),
      Phase.scheduleCollector(ROOTS),
      Phase.scheduleGlobal   (ROOTS),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),
      // refTypeClosurePhase

      Phase.scheduleCollector  (SOFT_REFS),
      Phase.scheduleGlobal     (REDIRECT_CLOSURE),
      Phase.scheduleCollector  (REDIRECT_CLOSURE),
      Phase.scheduleCollector  (WEAK_REFS),
      Phase.scheduleGlobal     (REDIRECT_CLOSURE),
      Phase.scheduleCollector  (REDIRECT_CLOSURE),
      Phase.scheduleCollector  (PHANTOM_REFS),

      Phase.scheduleMutator  (REMEMBERED_SETS),
      Phase.scheduleGlobal   (REMEMBERED_SETS),
      Phase.scheduleCollector(REMEMBERED_SETS),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),

      Phase.scheduleCollector  (FINALIZABLE),
      Phase.scheduleGlobal     (REDIRECT_CLOSURE),
      Phase.scheduleCollector  (REDIRECT_CLOSURE),

      Phase.scheduleComplex  (forwardPhase),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),

      Phase.scheduleCollector(CLEANUP_BLOCKS),
      Phase.scheduleCollector(CLEAR_CARD_META),

      Phase.scheduleMutator  (REDIRECT_RELEASE),
      Phase.scheduleCollector(REDIRECT_RELEASE),
      Phase.scheduleGlobal   (REDIRECT_RELEASE),

//      Phase.scheduleCollector(CLEAR_CARD_META),

      Phase.scheduleComplex  (finishPhase)
    );

  public static final short relocationPhase = Phase.createComplex("mature-evacuation", null,
      Phase.scheduleMutator  (REDIRECT_PREPARE),
      Phase.scheduleGlobal   (REDIRECT_PREPARE),
      Phase.scheduleCollector(REDIRECT_PREPARE),

      Phase.scheduleMutator  (PREPARE_STACKS),
      Phase.scheduleGlobal   (PREPARE_STACKS),
      Phase.scheduleCollector(STACK_ROOTS),
      Phase.scheduleGlobal   (STACK_ROOTS),
      Phase.scheduleCollector(ROOTS),
      Phase.scheduleGlobal   (ROOTS),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),
      Phase.scheduleCollector(SOFT_REFS),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),
      Phase.scheduleCollector(WEAK_REFS),
      Phase.scheduleCollector(FINALIZABLE),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),
      Phase.scheduleCollector(PHANTOM_REFS),
      Phase.scheduleComplex  (forwardPhase),

      Phase.scheduleMutator  (REMEMBERED_SETS),
      Phase.scheduleGlobal   (REMEMBERED_SETS),
      Phase.scheduleCollector(REMEMBERED_SETS),
      Phase.scheduleGlobal   (REDIRECT_CLOSURE),
      Phase.scheduleCollector(REDIRECT_CLOSURE),

      Phase.scheduleCollector(CLEANUP_BLOCKS),
      Phase.scheduleCollector(CLEAR_CARD_META),

      Phase.scheduleMutator  (REDIRECT_RELEASE),
      Phase.scheduleCollector(REDIRECT_RELEASE),
      Phase.scheduleGlobal   (REDIRECT_RELEASE)
  );

  public short matureCollection = Phase.createComplex("mature-collection", null,
    Phase.scheduleComplex  (initPhase),
    // Mark
    Phase.scheduleComplex  (rootClosurePhase),

    Phase.scheduleGlobal   (RELEASE),

    Phase.scheduleComplex  (relocationSetSelection),

    Phase.scheduleComplex  (relocationPhase),

    Phase.scheduleComplex  (finishPhase)
  );

  /**
   * Constructor
   */
  public G1() {
    collection = matureCollection;
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
    if (VM.VERIFY_ASSERTIONS) {
      Log.write("Global ");
      Log.writeln(Phase.getName(phaseId));
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
      //PureG1.stacksPrepared = false;
      if (currentGCKind != 0) {
        // FULL_GC
      } else if (collection == nurseryCollection) {
        currentGCKind = YOUNG_GC;
        VM.assertions.fail("Error: Young GC required");
      } else {
        currentGCKind = MIXED_GC;
      }

      if (currentGCKind == MIXED_GC) PauseTimePredictor.stopTheWorldStart();
      startTime = VM.statistics.nanoTime();
      ConcurrentRemSetRefinement.pause();
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION_PREPARE) {
      blocksSnapshot = regionSpace.snapshotBlocks(currentGCKind == YOUNG_GC);
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION) {
      if (currentGCKind == YOUNG_GC) {
        // blocksSnapshot is already and only contains young & survivor regions
        relocationSet = blocksSnapshot;
      } else {
        relocationSet = RegionSpace.computeRelocationBlocks(blocksSnapshot, true, false);
      }
      if (currentGCKind == MIXED_GC) {
        PauseTimePredictor.predict(relocationSet);
      }
      RegionSpace.markRegionsAsRelocate(relocationSet);
      blocksSnapshot = null;
      return;
    }

    if (phaseId == REDIRECT_PREPARE) {
      if (collection == nurseryCollection) {
        currentGCKind = YOUNG_GC;
      }
      if (nurseryGC()) {
        PauseTimePredictor.nurseryGCStart();
//        regionSpace.prepare();
        VM.memory.globalPrepareVMSpace();
      }
      // Flush mutators
      if (!nurseryGC()) ConcurrentRemSetRefinement.resume();
      VM.activePlan.resetMutatorIterator();
      G1Mutator m;
      while ((m = (G1Mutator) VM.activePlan.getNextMutator()) != null) {
        m.dropCurrentRSBuffer();
      }
      ConcurrentRemSetRefinement.pause();
      (nurseryGC() ? nurseryTrace : matureTrace).prepare();
      return;
    }

    if (phaseId == REMEMBERED_SETS) {
      VM.activePlan.resetMutatorIterator();
      G1Mutator m;
      while ((m = (G1Mutator) VM.activePlan.getNextMutator()) != null) {
        m.dropCurrentRSBuffer();
      }
      return;
    }

    if (phaseId == REDIRECT_CLOSURE) {
      return;
    }

    if (phaseId == REDIRECT_RELEASE) {
//      regionSpace.promoteAllRegionsAsOldGeneration();
      (nurseryGC() ? nurseryTrace : matureTrace).release();
      if (!nurseryGC()) {
        regionSpace.release();
        markTrace.release();
        super.collectionPhase(RELEASE);
      } else {
        VM.memory.globalReleaseVMSpace();
      }
      return;
    }

    if (phaseId == COMPLETE) {
      ConcurrentRemSetRefinement.resume();
      if (currentGCKind == MIXED_GC) PauseTimePredictor.stopTheWorldEnd();
      super.collectionPhase(COMPLETE);
      if (nurseryGC()) PauseTimePredictor.nurseryGCEnd();
      lastGCKind = currentGCKind;
      currentGCKind = NOT_IN_GC;
//      regionSpace.validate();
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

  final int TOTAL_LOGICAL_REGIONS = VM.AVAILABLE_END.diff(VM.AVAILABLE_START).toWord().rshl(Region.LOG_BYTES_IN_BLOCK).toInt();

  @Override
  protected boolean collectionRequired(boolean spaceFull, Space space) {
    // Young GC
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(TOTAL_LOGICAL_REGIONS > 0);


    if (GENERATIONAL && Phase.isPhaseStackEmpty() && (!Plan.gcInProgress()) && (!Phase.concurrentPhaseActive()) && (((float) regionSpace.youngRegions()) / ((float) TOTAL_LOGICAL_REGIONS) > newSizeRatio)) {
      Log.writeln("Nursery GC ");
      collection = nurseryCollection;
      return true;
    }

    // Full GC
    int usedPages = getPagesUsed() - metaDataSpace.reservedPages();
    int totalPages = getTotalPages() - metaDataSpace.reservedPages();
    boolean fullGCRequired = false;
    if ((totalPages - usedPages) < (totalPages * RESERVE_PERCENT)) {
      fullGCRequired = true;
    }
    fullGCRequired = fullGCRequired || super.collectionRequired(spaceFull, space);
    if (fullGCRequired) collection = matureCollection;
    return fullGCRequired;
  }

  @Override
  protected boolean concurrentCollectionRequired() {
    int usedPages = getPagesUsed() - metaDataSpace.reservedPages();
    int totalPages = getTotalPages() - metaDataSpace.reservedPages();
    boolean mixedGCRequired = !Phase.concurrentPhaseActive() && ((usedPages * 100) > (totalPages * INIT_HEAP_OCCUPANCY_PERCENT));
    if (mixedGCRequired) {
      Log.write("Mixed GC ", regionSpace.youngRegions());
      Log.writeln("/", TOTAL_LOGICAL_REGIONS);
      collection = matureCollection;
    }
    return mixedGCRequired;
  }

  float newSizeRatio = Options.g1NewSizePercent.getValue() / 100;
  final float RESERVE_PERCENT = Options.g1ReservePercent.getValue() / 100;
  final float INIT_HEAP_OCCUPANCY_PERCENT = Options.g1InitiatingHeapOccupancyPercent.getValue();

  //public static final float RESERVE_PERCENT = Options.g1ReservePercent.getValue() / 100;

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
