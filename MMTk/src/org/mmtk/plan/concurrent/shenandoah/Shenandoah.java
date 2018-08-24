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
import org.mmtk.policy.ForwardingTable;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.Space;
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
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class Shenandoah extends Concurrent {

  public static final RegionSpace regionSpace = new RegionSpace("region", VMRequest.discontiguous());
  public static final int RS = regionSpace.getDescriptor();
  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace evacuateTrace = new Trace(metaDataSpace);
  public static AddressArray relocationSet;
  public static boolean readBarrierEnabled = false;

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
  public static final int SCAN_EVACUATE = 1;

  /* Phases */
  public static final short EVACUATE_PREPARE = Phase.createSimple("evacuate-prepare");
  public static final short EVACUATE_CLOSURE = Phase.createSimple("evacuate-closure");
  public static final short EVACUATE_RELEASE = Phase.createSimple("evacuate-release");
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection");
  public static final short CLEANUP_BLOCKS = Phase.createSimple("cleanup-blocks");
  public static final short EVACUATE = Phase.createSimple("evacuate");

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

    Phase.scheduleComplex  (forwardPhase),

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

    // Evacuate
    Phase.scheduleGlobal   (EVACUATE_PREPARE),
    Phase.scheduleCollector(EVACUATE_PREPARE),
    Phase.scheduleCollector(EVACUATE),
    Phase.scheduleComplex  (forwardPhase),
    Phase.scheduleCollector(EVACUATE_RELEASE),
    Phase.scheduleGlobal   (EVACUATE_RELEASE),

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
    if (VM.VERIFY_ASSERTIONS) {
      Log.write("Global ");
      Log.writeln(Phase.getName(phaseId));
    }
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
      clearForwardingTables();
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION) {
      AddressArray blocksSnapshot = regionSpace.snapshotBlocks(false);
      relocationSet = RegionSpace.computeRelocationBlocks(blocksSnapshot, false, false);
      RegionSpace.markRegionsAsRelocate(relocationSet);
      return;
    }
//
    if (phaseId == EVACUATE_PREPARE) {
      evacuateTrace.prepare();
      return;
    }
//
//    if (phaseId == EVACUATE_CLOSURE) {
//      return;
//    }
//
    if (phaseId == EVACUATE_RELEASE) {
      evacuateTrace.release();
      return;
    }

    super.collectionPhase(phaseId);
  }

  @Inline
  private void clearForwardingTables() {
    if (relocationSet == null) return;
    for (int i = 0; i < relocationSet.length(); i++) {
      Address region = relocationSet.get(i);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!region.isZero());
      ForwardingTable.clear(region);
    }
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
    TransitiveClosure.registerSpecializedScan(SCAN_EVACUATE, ShenandoahEvacuateTraceLocal.class);
    super.registerSpecializedMethods();
  }

  @Override
  @Inline
  public ObjectReference loadObjectReference(Address slot) {
    return getForwardingPointer(slot.loadObjectReference());
  }

  @Override
  @Inline
  public void storeObjectReference(Address slot, ObjectReference value) {
    slot.store(getForwardingPointer(value));
  }



  @Inline
  public static ObjectReference getForwardingPointer(ObjectReference obj) {
    // Skip if barriers are disabled
    if (!Shenandoah.readBarrierEnabled) return obj;
    // Skip if `obj` is in VM Space
    if (VM.objectModel.objectStartRef(obj).LT(VM.AVAILABLE_START)) return obj;
    // Lookup the forwarding table
    ObjectReference rtn = obj;
    if (!obj.isNull() && Space.isInSpace(Shenandoah.RS, obj)) {
//      if (Region.relocationRequired(Region.of(obj))) {
//        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ForwardingWord.isForwarded(obj));
//        return ForwardingWord.extractForwardingPointer(VM.objectModel.readAvailableBitsWord(obj));
//      }
      ObjectReference forwarded = ForwardingTable.getForwardingPointer(obj);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(forwarded.isNull() || Space.isInSpace(Shenandoah.RS, forwarded));
      rtn = forwarded.isNull() ? obj : forwarded;
    }
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(obj.isNull() || !rtn.isNull());
      if (!obj.isNull()) {
        VM.assertions._assert(VM.debugging.validRef(obj));
        VM.assertions._assert(VM.debugging.validRef(rtn));
      }
    }
    return rtn;
  }
}
