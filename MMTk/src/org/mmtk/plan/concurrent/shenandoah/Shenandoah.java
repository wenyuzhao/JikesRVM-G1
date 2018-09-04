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
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.options.*;
import org.mmtk.utility.sanitychecker.SanityChecker;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

/**
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class Shenandoah extends Concurrent {

  public static final RegionSpace regionSpace = new RegionSpace("region", VMRequest.discontiguous());
  public static final int RS = regionSpace.getDescriptor();
  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace evacuateTrace = new Trace(metaDataSpace);
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
  public static final short SET_BROOKS_BARRIER_ACTIVE = Phase.createSimple("set-brooks-barrier-active");
  public static final short CLEAR_BROOKS_BARRIER_ACTIVE = Phase.createSimple("clear-brooks-barrier-active");
  // Evacuation phases
  public static final short EVACUATE_PREPARE = Phase.createSimple("evacuate-prepare");
  public static final short EVACUATE_RELEASE = Phase.createSimple("evacuate-release");
  public static final short EVACUATE = Phase.createSimple("evacuate");
  public static final short EVACUATE_ROOTS = Phase.createSimple("evacuate-roots");
  public static final short preemptConcurrentEvacuate = Phase.createComplex("preempt-concurrent-evacuate", null,
      Phase.scheduleCollector(EVACUATE));
  public static final short CONCURRENT_EVACUATE = Phase.createConcurrent("concurrent-evacuate",
      Phase.scheduleComplex(preemptConcurrentEvacuate));
  public static final short concurrentEvacuate = Phase.createComplex("concurrent-evacuate", null,
//      Phase.scheduleGlobal    (SET_BARRIER_ACTIVE),
//      Phase.scheduleMutator   (SET_BARRIER_ACTIVE),
//      Phase.scheduleCollector (FLUSH_COLLECTOR),
      Phase.scheduleGlobal    (SET_BROOKS_BARRIER_ACTIVE),
      Phase.scheduleConcurrent(CONCURRENT_EVACUATE),
//      Phase.scheduleCollector(EVACUATE),
      Phase.scheduleGlobal    (CLEAR_BROOKS_BARRIER_ACTIVE)
//      Phase.scheduleGlobal    (CLEAR_BARRIER_ACTIVE),
//      Phase.scheduleMutator   (CLEAR_BARRIER_ACTIVE)
  );
  // Update references phases
  public static final short SET_INDIRECT_BARRIER_ACTIVE = Phase.createSimple("set-indirect-barrier-active");
  public static final short CLEAR_INDIRECT_BARRIER_ACTIVE = Phase.createSimple("clear-indirect-barrier-active");
  public static final short FORWARD_PREPARE = Phase.createSimple("forward-prepare");
  public static final short FORWARD_CLOSURE = Phase.createSimple("forward-closure");
  public static final short FORWARD_RELEASE = Phase.createSimple("forward-release");
  public static final short preemptConcurrentForwardClosure = Phase.createComplex("preempt-concurrent-forward-trace", null,
      Phase.scheduleMutator  (FLUSH_MUTATOR),
      Phase.scheduleCollector(FORWARD_CLOSURE));
  public static final short CONCURRENT_FORWARD_CLOSURE = Phase.createConcurrent("concurrent-forward-closure",
      Phase.scheduleComplex(preemptConcurrentForwardClosure));
  public static final short concurrentForwardClosure = Phase.createComplex("concurrent-forward", null,
      Phase.scheduleGlobal    (SET_BARRIER_ACTIVE),
      Phase.scheduleGlobal    (SET_INDIRECT_BARRIER_ACTIVE),
      Phase.scheduleMutator   (SET_BARRIER_ACTIVE),
      Phase.scheduleCollector (FLUSH_COLLECTOR),
      Phase.scheduleConcurrent(CONCURRENT_FORWARD_CLOSURE),
      Phase.scheduleGlobal    (CLEAR_INDIRECT_BARRIER_ACTIVE),
      Phase.scheduleGlobal    (CLEAR_BARRIER_ACTIVE),
      Phase.scheduleMutator   (CLEAR_BARRIER_ACTIVE));
  // Eager cleanup phases
  public static final short EAGER_CLEANUP = Phase.createSimple("eager-cleanup");
  public static final short preemptConcurrentEagerCleanup = Phase.createComplex("preempt-concurrent-eager-cleanup", null,
      Phase.scheduleCollector(EAGER_CLEANUP));
  public static final short CONCURRENT_EAGER_CLEANUP = Phase.createConcurrent("concurrent-eager-cleanup",
      Phase.scheduleComplex(preemptConcurrentEagerCleanup));
  // Cleanup phases
  public static final short CLEANUP = Phase.createSimple("cleanup");
  public static final short preemptConcurrentCleanup = Phase.createComplex("preempt-concurrent-cleanup", null,
      Phase.scheduleCollector(CLEANUP));
  public static final short CONCURRENT_CLEANUP = Phase.createConcurrent("concurrent-cleanup",
      Phase.scheduleComplex(preemptConcurrentCleanup));

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
      Phase.scheduleGlobal   (EVACUATE_PREPARE),
      Phase.scheduleCollector(EVACUATE_PREPARE),
      Phase.scheduleMutator  (PREPARE_STACKS),
      Phase.scheduleGlobal   (PREPARE_STACKS),
      Phase.scheduleCollector(STACK_ROOTS),
      Phase.scheduleGlobal   (STACK_ROOTS),
      Phase.scheduleCollector(ROOTS),
      Phase.scheduleGlobal   (ROOTS),
      Phase.scheduleCollector(FLUSH_COLLECTOR),
      Phase.scheduleCollector(EVACUATE),
      Phase.scheduleCollector(EVACUATE_RELEASE),
      Phase.scheduleGlobal   (EVACUATE_RELEASE),
      // Update refs
      Phase.scheduleGlobal   (FORWARD_PREPARE),
      Phase.scheduleCollector(FORWARD_PREPARE),
      Phase.scheduleMutator  (FORWARD_PREPARE),
      Phase.scheduleMutator  (PREPARE_STACKS),
      Phase.scheduleGlobal   (PREPARE_STACKS),
      Phase.scheduleCollector(STACK_ROOTS),
      Phase.scheduleGlobal   (STACK_ROOTS),
      Phase.scheduleCollector(ROOTS),
      Phase.scheduleGlobal   (ROOTS),
      Phase.scheduleCollector(FORWARD_CLOSURE),
      Phase.scheduleCollector(SOFT_REFS),
      Phase.scheduleCollector(FORWARD_CLOSURE),
      Phase.scheduleCollector(WEAK_REFS),
      Phase.scheduleCollector(FINALIZABLE),
      Phase.scheduleCollector(FORWARD_CLOSURE),
      Phase.scheduleCollector(PHANTOM_REFS),
      Phase.scheduleComplex  (forwardPhase),
      Phase.scheduleCollector(FORWARD_CLOSURE),
      Phase.scheduleMutator  (FORWARD_RELEASE),
      Phase.scheduleCollector(FORWARD_RELEASE),
      Phase.scheduleGlobal   (FORWARD_RELEASE),
      // Cleanup
      Phase.scheduleCollector(CLEANUP),
      Phase.scheduleComplex  (finishPhase)
  );

  /**
   * Constructor
   */
  public Shenandoah() {
    collection = _collection;
  }

  @Override
  @Interruptible
  public void processOptions() {
    super.processOptions();
    /* Set up the concurrent marking phase */
    replacePhase(Phase.scheduleCollector(FORWARD_CLOSURE), Phase.scheduleComplex(concurrentForwardClosure));
    replacePhase(Phase.scheduleCollector(EAGER_CLEANUP), Phase.scheduleConcurrent(CONCURRENT_EAGER_CLEANUP));
    replacePhase(Phase.scheduleCollector(EVACUATE), Phase.scheduleComplex(concurrentEvacuate));
    replacePhase(Phase.scheduleCollector(CLEANUP), Phase.scheduleConcurrent(CONCURRENT_CLEANUP));
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
      AddressArray blocksSnapshot = regionSpace.snapshotRegions(false);
      relocationSet = RegionSpace.computeRelocationRegions(blocksSnapshot, false, false);
      RegionSpace.markRegionsAsRelocate(relocationSet);

      if (VM.VERIFY_ASSERTIONS) {
        Log.writeln("Relocation set size ", relocationSet.length());
      }
      return;
    }

    if (phaseId == EVACUATE_PREPARE) {
      evacuateTrace.prepare();
      return;
    }

    if (phaseId == EVACUATE_RELEASE) {
      evacuateTrace.release();
      return;
    }

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
      referenceUpdatingBarrierActive = false;
      return;
    }

    if (phaseId == SET_BROOKS_BARRIER_ACTIVE) {
      brooksBarrierActive = true;
      return;
    }

    if (phaseId == CLEAR_BROOKS_BARRIER_ACTIVE) {
      brooksBarrierActive = false;
      return;
    }

    if (phaseId == SET_INDIRECT_BARRIER_ACTIVE) {
//      referenceUpdatingBarrierActive = true;
      return;
    }

    if (phaseId == CLEAR_INDIRECT_BARRIER_ACTIVE) {
//      referenceUpdatingBarrierActive = false;
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
    int usedPages = getPagesUsed();// - metaDataSpace.reservedPages();
    int totalPages = getTotalPages();// - metaDataSpace.reservedPages();
    if ((totalPages - usedPages) < (totalPages * RESERVE_PERCENT)) {
      return true;
    }
    return super.collectionRequired(spaceFull, space);
  }

  @Override
  protected boolean concurrentCollectionRequired() {
    int usedPages = getPagesUsed();
    int totalPages = getTotalPages();
    return !Phase.concurrentPhaseActive() && ((usedPages * 100) > (totalPages * INIT_HEAP_OCCUPANCY_PERCENT));
  }

  final float RESERVE_PERCENT = 5 / 100;//Options.g1ReservePercent.getValue() / 100;
  final float INIT_HEAP_OCCUPANCY_PERCENT = 20f;//Options.g1InitiatingHeapOccupancyPercent.getValue();

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

  @Override
  @Inline
  public void storeObjectReference(Address slot, ObjectReference value) {
    slot.store(getForwardingPointer(value, false));
  }

  @Override
  @Inline
  public ObjectReference loadObjectReference(Address slot) {
    return getForwardingPointer(slot.loadObjectReference(), false);
  }

  static boolean referenceUpdatingBarrierActive = false;
  static boolean brooksBarrierActive = false;
  static Lock barrierLock = VM.newLock("barrierLock");

  @Inline
  static public ObjectReference getForwardingPointer(ObjectReference object, boolean access) {
    if (brooksBarrierActive) {
      if (object.isNull() || object.toAddress().LT(VM.AVAILABLE_START)) return object;
//      VM.assertions._assert(VM.debugging.validRef(object));
      if (true) {
        if (Space.isInSpace(RS, object) && Region.relocationRequired(Region.of(object))) {
          ObjectReference newObject;
          Word priorStatusWord = RegionSpace.ForwardingWord.attemptToForward(object);
          if (RegionSpace.ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
            newObject = RegionSpace.ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
            VM.assertions._assert(VM.debugging.validRef(newObject));
            VM.assertions._assert(!RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(newObject));
          } else {
            newObject = RegionSpace.ForwardingWord.forwardObjectWithinMutatorContext(object, ALLOC_RS);
            VM.assertions._assert(VM.debugging.validRef(newObject));
            VM.assertions._assert(!RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(newObject));
          }
          return newObject;
        } else {
          VM.assertions._assert(VM.debugging.validRef(object));
          VM.assertions._assert(!RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(object));
          return object;
//        ObjectReference forwarded = RegionSpace.ForwardingWord.getForwardedObject(object);
//        return forwarded.isNull() ? object : forwarded;
        }
      } else {
        return object;
//        ObjectReference forwarded = RegionSpace.ForwardingWord.getForwardedObject(object);
//        return forwarded.isNull() ? object : forwarded;
      }
    } else if (referenceUpdatingBarrierActive) {
      if (object.isNull() || object.toAddress().LT(VM.AVAILABLE_START)) return object;
//      if (Space.isInSpace(RS, object) && Region.relocationRequired(Region.of(object))) {
//        VM.assertions._assert(RegionSpace.ForwardingWord.isForwarded(object));
//      }
//      if (!access) return object;
      ObjectReference forwarded = RegionSpace.ForwardingWord.getForwardedObject(object);
      object = forwarded.isNull() ? object : forwarded;
      VM.assertions._assert(VM.debugging.validRef(object));
      return object;
    } else {
//      VM.assertions._assert(VM.debugging.validRef(object));
      return object;
    }
  }
}
