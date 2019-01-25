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
package org.mmtk.plan.regional.rust;

import org.mmtk.plan.Plan;
import org.mmtk.plan.Phase;
import org.mmtk.plan.Trace;
import org.mmtk.plan.ComplexPhase;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.options.*;
import org.mmtk.utility.sanitychecker.SanityChecker;
import org.mmtk.utility.statistics.Timer;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class Regional extends Plan {
  /* Shared Timers */
  private static final Timer refTypeTime = new Timer("refType", false, true);
  private static final Timer scanTime = new Timer("scan", false, true);
  private static final Timer finalizeTime = new Timer("finalize", false, true);

  /* Phases */
  public static final short SET_COLLECTION_KIND = Phase.createSimple("set-collection-kind", null);
  public static final short INITIATE            = Phase.createSimple("initiate", null);
  public static final short PREPARE             = Phase.createSimple("prepare");
  public static final short PREPARE_STACKS      = Phase.createSimple("prepare-stacks", null);
  public static final short STACK_ROOTS         = Phase.createSimple("stacks");
  public static final short ROOTS               = Phase.createSimple("root");
  public static final short CLOSURE             = Phase.createSimple("closure", scanTime);
  public static final short SOFT_REFS           = Phase.createSimple("soft-ref", refTypeTime);
  public static final short WEAK_REFS           = Phase.createSimple("weak-ref", refTypeTime);
  public static final short FINALIZABLE         = Phase.createSimple("finalize", finalizeTime);
  public static final short WEAK_TRACK_REFS     = Phase.createSimple("weak-track-ref", refTypeTime);
  public static final short PHANTOM_REFS        = Phase.createSimple("phantom-ref", refTypeTime);
  public static final short FORWARD             = Phase.createSimple("forward");
  public static final short FORWARD_REFS        = Phase.createSimple("forward-ref", refTypeTime);
  public static final short FORWARD_FINALIZABLE = Phase.createSimple("forward-finalize", finalizeTime);
  public static final short RELEASE             = Phase.createSimple("release");
  public static final short COMPLETE            = Phase.createSimple("complete", null);

  /* Sanity placeholder */
  public static final short PRE_SANITY_PLACEHOLDER  = Phase.createSimple("pre-sanity-placeholder", null);
  public static final short POST_SANITY_PLACEHOLDER = Phase.createSimple("post-sanity-placeholder", null);

  /* Sanity phases */
  public static final short SANITY_SET_PREGC    = Phase.createSimple("sanity-setpre", null);
  public static final short SANITY_SET_POSTGC   = Phase.createSimple("sanity-setpost", null);
  public static final short SANITY_PREPARE      = Phase.createSimple("sanity-prepare", null);
  public static final short SANITY_ROOTS        = Phase.createSimple("sanity-roots", null);
  public static final short SANITY_COPY_ROOTS   = Phase.createSimple("sanity-copy-roots", null);
  public static final short SANITY_BUILD_TABLE  = Phase.createSimple("sanity-build-table", null);
  public static final short SANITY_CHECK_TABLE  = Phase.createSimple("sanity-check-table", null);
  public static final short SANITY_RELEASE      = Phase.createSimple("sanity-release", null);

  // CHECKSTYLE:OFF

  /** Ensure stacks are ready to be scanned */
  protected static final short prepareStacks = Phase.createComplex("prepare-stacks", null,
      Phase.scheduleMutator    (PREPARE_STACKS),
      Phase.scheduleGlobal     (PREPARE_STACKS));

  /** Trace and set up a sanity table */
  protected static final short sanityBuildPhase = Phase.createComplex("sanity-build", null,
      Phase.scheduleGlobal     (SANITY_PREPARE),
      Phase.scheduleCollector  (SANITY_PREPARE),
      Phase.scheduleComplex    (prepareStacks),
      Phase.scheduleCollector  (SANITY_ROOTS),
      Phase.scheduleGlobal     (SANITY_ROOTS),
      Phase.scheduleCollector  (SANITY_COPY_ROOTS),
      Phase.scheduleGlobal     (SANITY_BUILD_TABLE));

  /** Validate a sanity table */
  protected static final short sanityCheckPhase = Phase.createComplex("sanity-check", null,
      Phase.scheduleGlobal     (SANITY_CHECK_TABLE),
      Phase.scheduleCollector  (SANITY_RELEASE),
      Phase.scheduleGlobal     (SANITY_RELEASE));

  /** Start the collection, including preparation for any collected spaces. */
  protected static final short initPhase = Phase.createComplex("init",
      Phase.scheduleGlobal     (SET_COLLECTION_KIND),
      Phase.scheduleGlobal     (INITIATE),
      Phase.schedulePlaceholder(PRE_SANITY_PLACEHOLDER));

  /**
   * Perform the initial determination of liveness from the roots.
   */
  protected static final short rootClosurePhase = Phase.createComplex("initial-closure", null,
      Phase.scheduleMutator    (PREPARE),
      Phase.scheduleGlobal     (PREPARE),
      Phase.scheduleCollector  (PREPARE),
      Phase.scheduleComplex    (prepareStacks),
      Phase.scheduleCollector  (STACK_ROOTS),
      Phase.scheduleGlobal     (STACK_ROOTS),
      Phase.scheduleCollector  (ROOTS),
      Phase.scheduleGlobal     (ROOTS),
      Phase.scheduleGlobal     (CLOSURE),
      Phase.scheduleCollector  (CLOSURE));

  /**
   * Complete closure including reference types and finalizable objects.
   */
  protected static final short refTypeClosurePhase = Phase.createComplex("refType-closure", null,
      Phase.scheduleCollector  (SOFT_REFS),
      Phase.scheduleGlobal     (CLOSURE),
      Phase.scheduleCollector  (CLOSURE),
      Phase.scheduleCollector  (WEAK_REFS),
      Phase.scheduleCollector  (FINALIZABLE),
      Phase.scheduleGlobal     (CLOSURE),
      Phase.scheduleCollector  (CLOSURE),
      Phase.schedulePlaceholder(WEAK_TRACK_REFS),
      Phase.scheduleCollector  (PHANTOM_REFS));

  /**
   * Ensure that all references in the system are correct.
   */
  protected static final short forwardPhase = Phase.createComplex("forward-all", null,
      /* Finish up */
      Phase.schedulePlaceholder(FORWARD),
      Phase.scheduleCollector  (FORWARD_REFS),
      Phase.scheduleCollector  (FORWARD_FINALIZABLE));

  /**
   * Complete closure including reference types and finalizable objects.
   */
  protected static final short completeClosurePhase = Phase.createComplex("release", null,
      Phase.scheduleMutator    (RELEASE),
      Phase.scheduleCollector  (RELEASE),
      Phase.scheduleGlobal     (RELEASE));


  /**
   * The collection scheme - this is a small tree of complex phases.
   */
  protected static final short finishPhase = Phase.createComplex("finish",
      Phase.schedulePlaceholder(POST_SANITY_PLACEHOLDER),
      Phase.scheduleCollector  (COMPLETE),
      Phase.scheduleGlobal     (COMPLETE));

  /**
   * This is the phase that is executed to perform a collection.
   */
  public short collection = Phase.createComplex("collection", null,
      Phase.scheduleComplex(initPhase),
      Phase.scheduleComplex(rootClosurePhase),
      Phase.scheduleComplex(refTypeClosurePhase),
      Phase.scheduleComplex(forwardPhase),
      Phase.scheduleComplex(completeClosurePhase),
      Phase.scheduleComplex(finishPhase));

  public static final RegionSpace regionSpace = new RegionSpace("region", VMRequest.discontiguous());
  public static final int RS = regionSpace.getDescriptor();
  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace evacuateTrace = new Trace(metaDataSpace);
  public static AddressArray blocksSnapshot, relocationSet;
  //public static boolean concurrentMarkingInProgress = false;

  static {
    Options.g1ReservePercent = new G1ReservePercent();
    regionSpace.makeAllocAsMarked();
    smallCodeSpace.makeAllocAsMarked();
    nonMovingSpace.makeAllocAsMarked();
  }

  public static final int ALLOC_MC = Plan.ALLOC_DEFAULT;
  public static final int SCAN_MARK = 0;
  public static final int SCAN_EVACUATE = 1;

  /* Phases */
  public static final short EAGER_CLEANUP = Phase.createSimple("eager-cleanup");
  public static final short EVACUATE_PREPARE = Phase.createSimple("evacuate-prepare");
  public static final short EVACUATE_CLOSURE = Phase.createSimple("evacuate-closure");
  public static final short EVACUATE_RELEASE = Phase.createSimple("evacuate-release");
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection");
  public static final short CLEANUP_BLOCKS = Phase.createSimple("cleanup-blocks");

  public static final short evacuatePhase = Phase.createComplex("evacuate", null,
    Phase.scheduleMutator  (EVACUATE_PREPARE),
    Phase.scheduleGlobal   (EVACUATE_PREPARE),
    Phase.scheduleCollector(EVACUATE_PREPARE),
    // Roots
    Phase.scheduleMutator  (PREPARE_STACKS),
    Phase.scheduleGlobal   (PREPARE_STACKS),
    Phase.scheduleCollector(STACK_ROOTS),
    Phase.scheduleGlobal   (STACK_ROOTS),
    Phase.scheduleCollector(ROOTS),
    Phase.scheduleGlobal   (ROOTS),
    Phase.scheduleGlobal   (EVACUATE_CLOSURE),
    Phase.scheduleCollector(EVACUATE_CLOSURE),
    // Refs
    Phase.scheduleCollector(SOFT_REFS),
    Phase.scheduleGlobal   (EVACUATE_CLOSURE),
    Phase.scheduleCollector(EVACUATE_CLOSURE),
    Phase.scheduleCollector(WEAK_REFS),
    Phase.scheduleCollector(FINALIZABLE),
    Phase.scheduleGlobal   (EVACUATE_CLOSURE),
    Phase.scheduleCollector(EVACUATE_CLOSURE),
    Phase.scheduleCollector(PHANTOM_REFS),

    Phase.scheduleMutator  (EVACUATE_RELEASE),
    Phase.scheduleCollector(EVACUATE_RELEASE),
    Phase.scheduleGlobal   (EVACUATE_RELEASE)
  );




  public short _collection = Phase.createComplex("_collection", null,
    Phase.scheduleComplex  (initPhase),
    // Mark
    Phase.scheduleComplex  (rootClosurePhase),
    Phase.scheduleComplex  (refTypeClosurePhase),
    Phase.scheduleComplex  (completeClosurePhase),

    // Select relocation sets
    Phase.scheduleGlobal   (RELOCATION_SET_SELECTION),
    Phase.scheduleCollector(EAGER_CLEANUP),

    // Evacuate
    Phase.scheduleComplex  (evacuatePhase),

    // Cleanup
    Phase.scheduleCollector(CLEANUP_BLOCKS),

    Phase.scheduleComplex  (finishPhase)
  );

  /**
   * Constructor
   */
  public Regional() {
    collection = _collection;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId) {
    if (phaseId == SET_COLLECTION_KIND) {
      collectionAttempt = Plan.isUserTriggeredCollection() ? 1 : Allocator.determineCollectionAttempts();
      emergencyCollection = !Plan.isInternalTriggeredCollection() &&
          lastCollectionWasExhaustive() && collectionAttempt > 1;
      if (emergencyCollection) {
        if (Options.verbose.getValue() >= 1) Log.write("[Emergency]");
        forceFullHeapCollection();
      }
      return;
    }

    if (phaseId == INITIATE) {
      setGCStatus(GC_PREPARE);
      return;
    }

    if (phaseId == PREPARE_STACKS) {
      stacksPrepared = true;
      return;
    }

    if (phaseId == PREPARE) {
      loSpace.prepare(true);
      nonMovingSpace.prepare(true);
      if (USE_CODE_SPACE) {
        smallCodeSpace.prepare(true);
        largeCodeSpace.prepare(true);
      }
      immortalSpace.prepare();
      VM.memory.globalPrepareVMSpace();
      regionSpace.prepare();
      markTrace.prepareNonBlocking();
      return;
    }

    if (phaseId == STACK_ROOTS) {
      VM.scanning.notifyInitialThreadScanComplete(false);
      setGCStatus(GC_PROPER);
      return;
    }

    if (phaseId == ROOTS) {
      VM.scanning.resetThreadCounter();
      setGCStatus(GC_PROPER);
      return;
    }

    if (phaseId == CLOSURE) {
      return;
    }

    if (phaseId == RELEASE) {
      markTrace.release();
      loSpace.release(true);
      nonMovingSpace.release();
      if (USE_CODE_SPACE) {
        smallCodeSpace.release();
        largeCodeSpace.release(true);
      }
      immortalSpace.release();
      VM.memory.globalReleaseVMSpace();
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION) {
      AddressArray blocksSnapshot = regionSpace.snapshotRegions(false);
      relocationSet = RegionSpace.computeRelocationRegions(blocksSnapshot, false, false);
      RegionSpace.markRegionsAsRelocate(relocationSet);
      return;
    }

    if (phaseId == EVACUATE_PREPARE) {
      loSpace.prepare(true);
      nonMovingSpace.prepare(true);
      if (USE_CODE_SPACE) {
        smallCodeSpace.prepare(true);
        largeCodeSpace.prepare(true);
      }
      immortalSpace.prepare();
      VM.memory.globalPrepareVMSpace();
      regionSpace.prepare();
      evacuateTrace.prepare();
      return;
    }

    if (phaseId == EVACUATE_CLOSURE) {
      return;
    }

    if (phaseId == EVACUATE_RELEASE) {
      evacuateTrace.release();
      regionSpace.release();
      loSpace.release(true);
      nonMovingSpace.release();
      if (USE_CODE_SPACE) {
        smallCodeSpace.release();
        largeCodeSpace.release(true);
      }
      immortalSpace.release();
      VM.memory.globalReleaseVMSpace();
      return;
    }

    if (phaseId == COMPLETE) {
      setGCStatus(NOT_IN_GC);
      return;
    }

    if (Options.sanityCheck.getValue() && sanityChecker.collectionPhase(phaseId)) {
      return;
    }

    Log.write("Global phase ");
    Log.write(Phase.getName(phaseId));
    Log.writeln(" not handled.");
    VM.assertions.fail("Global phase not handled!");
  }


  /****************************************************************************
   *
   * Accounting
   */

  final int BOOT_PAGES = VM.AVAILABLE_START.diff(VM.HEAP_START).toInt() / Constants.BYTES_IN_PAGE;
  final float RESERVE_PERCENT = Options.g1ReservePercent.getValue() / 100f;

  @Override
  @Inline
  protected boolean collectionRequired(boolean spaceFull, Space space) {
    final int totalPages = getTotalPages();
    final boolean heapFull = ((totalPages - getPagesReserved()) * 10) < totalPages;
    return spaceFull || heapFull;
//    int totalPages = getTotalPages();
//    if (getPagesAvail() * 10 < totalPages) {
//      return true;
//    }
//    boolean heapFull = getPagesReserved() > totalPages;
//    return spaceFull || heapFull;


//    int totalPages = getTotalPages();
//    if (getPagesAvail() - BOOT_PAGES < totalPages * RESERVE_PERCENT) {
//      return true;
//    }
//    return super.collectionRequired(spaceFull, space);
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
  public int sanityExpectedRC(ObjectReference object, int sanityRootRC) {
    Space space = Space.getSpaceForObject(object);
    // Nursery
    if (space == regionSpace) {
      // We are never sure about objects in RS.
      // This is not very satisfying but allows us to use the sanity checker to
      // detect dangling pointers.
      return SanityChecker.UNSURE;
    }
    return super.sanityExpectedRC(object, sanityRootRC);
  }

  @Override
  @Inline
  public int getPagesUsed() {
    return super.getPagesUsed() + regionSpace.reservedPages();
  }

  @Override
  public boolean willNeverMove(ObjectReference object) {
    if (Space.isInSpace(RS, object)) return false;
    return super.willNeverMove(object);
  }

  @Override
  @Interruptible
  protected void registerSpecializedMethods() {
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, RegionalMarkTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_EVACUATE, RegionalEvacuateTraceLocal.class);
    super.registerSpecializedMethods();
  }


  // CHECKSTYLE:ON

  /**
   * The current collection attempt.
   */
  protected int collectionAttempt;

  /****************************************************************************
   * Collection
   */
  /**
   * Update the nursery zeroing approach based on option settings.
   *
   * @param nurserySpace The space to apply the changes to.
   */
  protected void switchNurseryZeroingApproach(Space nurserySpace) {
    if (Options.nurseryZeroing.getConcurrent()) {
      if (Options.nurseryZeroing.getAdaptive() &&
          (VM.collection.getActiveThreads() >= VM.collection.getDefaultThreads())) {
        // Many (non-daemon) threads, so we revert to bulk zeroing.
        nurserySpace.skipConcurrentZeroing();
      } else {
        nurserySpace.triggerConcurrentZeroing();
      }
    }
  }

  /**
   * {@inheritDoc}
   * Used for example to replace a placeholder.
   */
  @Override
  public void replacePhase(int oldScheduledPhase, int newScheduledPhase) {
    ComplexPhase cp = (ComplexPhase)Phase.getPhase(collection);
    cp.replacePhase(oldScheduledPhase, newScheduledPhase);
  }

  /**
   * Replace a placeholder phase.
   *
   * @param placeHolderPhase The placeholder phase
   * @param newScheduledPhase The new scheduled phase.
   */
  public void replacePlaceholderPhase(short placeHolderPhase, int newScheduledPhase) {
    ComplexPhase cp = (ComplexPhase)Phase.getPhase(collection);
    cp.replacePhase(Phase.schedulePlaceholder(placeHolderPhase), newScheduledPhase);
  }
}
