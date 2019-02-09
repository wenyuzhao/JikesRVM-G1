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
package org.mmtk.plan.regional.bakers;

import org.mmtk.plan.*;
import org.mmtk.plan.concurrent.Concurrent;
import org.mmtk.policy.RawPageSpace;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
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
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class G1 extends StopTheWorld {

  public static final RawPageSpace offHeapMetaDataSpace = new RawPageSpace("offheap-meta", VMRequest.discontiguous());
  public static final RegionSpace regionSpace = new RegionSpace("region", VMRequest.discontiguous());
  public static final int RS = regionSpace.getDescriptor();
  public final Trace markTrace = new Trace(offHeapMetaDataSpace);
  public final Trace evacuateTrace = new Trace(metaDataSpace);
  public static AddressArray blocksSnapshot, relocationSet;
  //public static boolean concurrentMarkingInProgress = false;

  static {
    Options.g1ReservePercent = new G1ReservePercent();
    Options.g1InitiatingHeapOccupancyPercent = new G1InitiatingHeapOccupancyPercent();
    Options.g1GCLiveThresholdPercent = new G1GCLiveThresholdPercent();
    Options.g1MaxNewSizePercent = new G1MaxNewSizePercent();
    Options.g1NewSizePercent = new G1NewSizePercent();
    Options.g1HeapWastePercent = new G1HeapWastePercent();
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
  public G1() {
    collection = _collection;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId) {
    if (phaseId == PREPARE) {
      G1Mutator.newMutatorBarrierActive = false;
      super.collectionPhase(PREPARE);
      regionSpace.prepare();
      markTrace.prepareNonBlocking();
      return;
    }

    if (phaseId == CLOSURE) {
      return;
    }

    if (phaseId == RELEASE) {
      markTrace.release();
      super.collectionPhase(RELEASE);
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION) {
      AddressArray blocksSnapshot = regionSpace.snapshotRegions(false);
      relocationSet = RegionSpace.computeRelocationRegions(blocksSnapshot, false, false);
      RegionSpace.markRegionsAsRelocate(relocationSet);
      return;
    }

    if (phaseId == EVACUATE_PREPARE) {
      super.collectionPhase(PREPARE);
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
      super.collectionPhase(RELEASE);
      G1Mutator.newMutatorBarrierActive = true;
      return;
    }

    super.collectionPhase(phaseId);
  }

  /****************************************************************************
   *
   * Accounting
   */

  final int BOOT_PAGES = VM.AVAILABLE_START.diff(VM.HEAP_START).toInt() / Constants.BYTES_IN_PAGE;
  final float RESERVE_PERCENT = Options.g1ReservePercent.getValue() / 100f;

  @Override
  protected boolean collectionRequired(boolean spaceFull, Space space) {
//    int totalPages = getTotalPages();
//    if (getPagesAvail() - BOOT_PAGES < totalPages * RESERVE_PERCENT) {
//      return true;
//    }
//    return super.collectionRequired(spaceFull, space);
    final int totalPages = getTotalPages();
    final boolean heapFull = ((totalPages - getPagesReserved()) * 10) < totalPages;
    return spaceFull || heapFull;
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
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, G1MarkTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_EVACUATE, G1EvacuateTraceLocal.class);
    super.registerSpecializedMethods();
  }
}
