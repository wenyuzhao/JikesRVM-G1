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
package org.mmtk.plan.concurrent.shenandoah;

import org.mmtk.plan.Phase;
import org.mmtk.plan.Plan;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.plan.concurrent.Concurrent;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.options.*;
import org.mmtk.utility.sanitychecker.SanityChecker;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class Shenandoah extends Concurrent {

  public static final RegionSpace regionSpace = new RegionSpace("region", VMRequest.discontiguous());
  public static final int RS = regionSpace.getDescriptor();
  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace forwardTrace = new Trace(metaDataSpace);
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

  public static final int ALLOC_RS = Plan.ALLOC_DEFAULT;
  public static final int SCAN_MARK = 0;
  public static final int SCAN_FORWARD = 1;

  /* Phases */
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection");
  public static final short EVACUATE = Phase.createSimple("evacuate");
  public static final short FORWARD_PREPARE = Phase.createSimple("forward-prepare");
  public static final short FORWARD_CLOSURE = Phase.createSimple("forward-closure");
  public static final short FORWARD_RELEASE = Phase.createSimple("forward-release");
  public static final short CLEANUP_BLOCKS = Phase.createSimple("cleanup-blocks");
  protected static final short preemptConcurrentForwardClosure = Phase.createComplex("preeempt-concurrent-forward-trace", null,
      Phase.scheduleMutator  (FLUSH_MUTATOR),
      Phase.scheduleCollector(FORWARD_CLOSURE));
  public static final short CONCURRENT_FORWARD_CLOSURE = Phase.createConcurrent("concurrent-forward-closure",
      Phase.scheduleComplex(preemptConcurrentForwardClosure));


  public short _collection = Phase.createComplex("_collection", null,
    Phase.scheduleComplex  (initPhase),
    // Mark
    Phase.scheduleComplex  (rootClosurePhase),
    Phase.scheduleComplex  (refTypeClosurePhase),
    Phase.scheduleComplex  (completeClosurePhase),
    // Select relocation sets
    Phase.scheduleGlobal   (RELOCATION_SET_SELECTION),
    // Evacuate
    Phase.scheduleCollector(EVACUATE),
    // Update refs
    Phase.scheduleGlobal   (FORWARD_PREPARE),
    Phase.scheduleCollector(FORWARD_PREPARE),
    Phase.scheduleMutator  (PREPARE),
    Phase.scheduleMutator  (PREPARE_STACKS),
    Phase.scheduleGlobal   (PREPARE_STACKS),
    Phase.scheduleCollector(STACK_ROOTS),
    Phase.scheduleGlobal   (STACK_ROOTS),
    Phase.scheduleCollector(ROOTS),
    Phase.scheduleGlobal   (ROOTS),
    Phase.scheduleCollector(FORWARD_CLOSURE),
//    Phase.scheduleConcurrent(CONCURRENT_FORWARD_CLOSURE),
    Phase.scheduleCollector(SOFT_REFS),
    Phase.scheduleCollector(FORWARD_CLOSURE),
//    Phase.scheduleConcurrent(CONCURRENT_FORWARD_CLOSURE),
    Phase.scheduleCollector(WEAK_REFS),
    Phase.scheduleCollector(FINALIZABLE),
    Phase.scheduleCollector(FORWARD_CLOSURE),
//    Phase.scheduleConcurrent(CONCURRENT_FORWARD_CLOSURE),
    Phase.scheduleCollector(PHANTOM_REFS),
    Phase.scheduleComplex  (forwardPhase),
    Phase.scheduleMutator  (RELEASE),
    Phase.scheduleCollector(FORWARD_RELEASE),
    Phase.scheduleGlobal   (FORWARD_RELEASE),
    // Cleanup
    Phase.scheduleCollector(CLEANUP_BLOCKS),
    Phase.scheduleComplex  (finishPhase)
  );

  /**
   * Constructor
   */
  public Shenandoah() {
    collection = _collection;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId) {
    if (phaseId == PREPARE) {
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
      AddressArray blocksSnapshot = regionSpace.snapshotBlocks(false);
      relocationSet = RegionSpace.computeRelocationBlocks(blocksSnapshot, false, false);
      RegionSpace.markRegionsAsRelocate(relocationSet);
      return;
    }

//    if (phaseId == EVACUATE_PREPARE) {
//      super.collectionPhase(PREPARE);
//      regionSpace.prepare();
//      forwardTrace.prepare();
//      return;
//    }
//
//    if (phaseId == EVACUATE_CLOSURE) {
//      return;
//    }
//
//    if (phaseId == EVACUATE_RELEASE) {
//      forwardTrace.release();
//      regionSpace.release();
//      super.collectionPhase(RELEASE);
//      return;
//    }

    if (phaseId == FORWARD_PREPARE) {
      super.collectionPhase(PREPARE);
      forwardTrace.prepare();
      regionSpace.prepare();
      return;
    }
    if (phaseId == FORWARD_RELEASE) {
      forwardTrace.release();
      regionSpace.release();
      super.collectionPhase(RELEASE);
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
    if ((totalPages - usedPages) < (totalPages * RESERVE_PERCENT)) {
      return true;
    }
    return super.collectionRequired(spaceFull, space);
  }

  @Override
  protected boolean concurrentCollectionRequired() {
    int usedPages = getPagesUsed() - metaDataSpace.reservedPages();
    int totalPages = getTotalPages() - metaDataSpace.reservedPages();
    return !Phase.concurrentPhaseActive() && ((usedPages * 100) > (totalPages * INIT_HEAP_OCCUPANCY_PERCENT));
  }

  final float RESERVE_PERCENT = Options.g1ReservePercent.getValue() / 100;
  final float INIT_HEAP_OCCUPANCY_PERCENT = Options.g1InitiatingHeapOccupancyPercent.getValue();

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
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, ShenandoahMarkTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_FORWARD, ShenandoahForwardTraceLocal.class);
    super.registerSpecializedMethods();
  }
}
