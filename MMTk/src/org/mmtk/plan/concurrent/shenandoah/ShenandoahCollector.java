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

import org.mmtk.plan.*;
import org.mmtk.plan.concurrent.ConcurrentCollector;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.pragma.Unpreemptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
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
 * See {@link Shenandoah} for an overview of the semi-space algorithm.
 *
 * @see Shenandoah
 * @see ShenandoahMutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class ShenandoahCollector extends ConcurrentCollector {

  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator copy = new RegionAllocator(Shenandoah.regionSpace, Region.SURVIVOR);
  protected final ShenandoahMarkTraceLocal markTrace = new ShenandoahMarkTraceLocal(global().markTrace);
  protected final ShenandoahEvacuateTraceLocal evacuateTrace = new ShenandoahEvacuateTraceLocal(global().evacuateTrace);
  protected final ShenandoahForwardTraceLocal forwardTrace = new ShenandoahForwardTraceLocal(global().forwardTrace);
  protected final ShenandoahValidateTraceLocal validateTrace = new ShenandoahValidateTraceLocal(global().validateTrace);
  protected final EvacuationLinearScan evacuationLinearScan = new EvacuationLinearScan();
  private static final short TRACE_MARK = 0;
  private static final short TRACE_EVACUATE = 1;
  private static final short TRACE_FORWARD = 2;
  private static final short TRACE_VALIDATE = 3;
  private short currentTrace;// = TRACE_MARK;
//  protected TraceLocal currentTrace;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public ShenandoahCollector() {}

  /****************************************************************************
   *
   * Collection-time allocation
   */
  private static boolean concurrentEvacuationExecuted = false;
  private static boolean concurrentRelocationSetSelectionExecuted = false;
  private static boolean concurrentEagerCleanupExecuted = false;
  private static boolean concurrentCleanupExecuted = false;
  private static final Atomic.Int atomicCounter = new Atomic.Int();

  @Inline
  private void evacuateRegions(boolean concurrent) {
    atomicCounter.set(0);
//    Shenandoah.referenceUpdatingBarrierActive = true;
    if (rendezvous() == 0) {
      VM.activePlan.resetMutatorIterator();
      ShenandoahMutator m;
      while ((m = (ShenandoahMutator) VM.activePlan.getNextMutator()) != null) {
        m.ra.reset();
      }
    }
    copy.reset();
    rendezvous();
    int index;
    while ((index = atomicCounter.add(1)) < Shenandoah.relocationSet.length()) {
      Address region = Shenandoah.relocationSet.get(index);
      if (!region.isZero()) {
//        Log.writeln("Evacuating ", region);
        Region.linearScan(evacuationLinearScan, region);
      }
      if (concurrent && group.isAborted()) {
        break;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address allocCopy(ObjectReference original, int bytes, int align, int offset, int allocator) {
    Address addr = copy.alloc(bytes, align, offset);
    return addr;
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    Shenandoah.regionSpace.postCopy(object, bytes);
    Shenandoah.initializeIndirectionPointer(object);
  }

  /****************************************************************************
   *
   * Collection
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    if (VM.VERIFY_ASSERTIONS) Log.writeln(Phase.getName(phaseId));
    if (phaseId == Shenandoah.PREPARE) {
      currentTrace = TRACE_MARK;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == Shenandoah.CLOSURE) {
      markTrace.completeTrace();
      return;
    }

    if (phaseId == Shenandoah.RELEASE) {
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      concurrentEvacuationExecuted = false;
      concurrentRelocationSetSelectionExecuted = false;
      concurrentEagerCleanupExecuted = false;
      concurrentCleanupExecuted = false;
      return;
    }

    if (phaseId == Shenandoah.RELOCATION_SET_SELECTION) {
      if (!concurrentRelocationSetSelectionExecuted && primary) {
        AddressArray blocksSnapshot = Shenandoah.regionSpace.snapshotRegions(false);
        Shenandoah.relocationSet = RegionSpace.computeRelocationRegions(blocksSnapshot, false, false);
        RegionSpace.markRegionsAsRelocate(Shenandoah.relocationSet);
      }
      rendezvous();
      return;
    }

    if (phaseId == Shenandoah.EAGER_CLEANUP) {
      if (concurrentEagerCleanupExecuted) return;
      atomicCounter.set(0);
      rendezvous();
      int index;
      while ((index = atomicCounter.add(1)) < Shenandoah.relocationSet.length()) {
        Address region = Shenandoah.relocationSet.get(index);
        if (!region.isZero() && Region.usedSize(region) == 0) {
          Shenandoah.relocationSet.set(index, Address.zero());
          Shenandoah.regionSpace.release(region);
        }
      }
      return;
    }

    if (phaseId == Shenandoah.EVACUATE_PREPARE) {
      currentTrace = TRACE_EVACUATE;
      evacuateTrace.prepare();
      copy.reset();
      return;
    }

    if (phaseId == Shenandoah.EVACUATE) {
      if (!concurrentEvacuationExecuted) {
        evacuateRegions(false);
        rendezvous();
      } else {
        int index;
        while ((index = atomicCounter.add(1)) < Shenandoah.relocationSet.length()) {
          Address region = Shenandoah.relocationSet.get(index);
          if (!region.isZero()) {
            Shenandoah.relocationSet.set(index, Address.zero());
            Region.setRelocationState(region, false);
          }
        }
      }
      return;
    }

    if (phaseId == Shenandoah.EVACUATE_RELEASE) {
      evacuateTrace.release();
      copy.reset();
      return;
    }

    if (phaseId == Shenandoah.FORWARD_PREPARE) {
      currentTrace = TRACE_FORWARD;
      forwardTrace.prepare();
      copy.reset();
      super.collectionPhase(Shenandoah.PREPARE, primary);
      return;
    }

    if (phaseId == Shenandoah.FORWARD_CLOSURE) {
      forwardTrace.completeTrace();
      return;
    }

    if (phaseId == Shenandoah.FORWARD_RELEASE) {
      forwardTrace.release();
      copy.reset();
      super.collectionPhase(Shenandoah.RELEASE, primary);
      return;
    }

//    if (phaseId == Shenandoah.VALIDATE_PREPARE) {
//      currentTrace = TRACE_VALIDATE;
//      validateTrace.prepare();
//      copy.reset();
//      super.collectionPhase(Shenandoah.PREPARE, primary);
//      return;
//    }
//
//    if (phaseId == Shenandoah.VALIDATE_CLOSURE) {
//      validateTrace.completeTrace();
//      return;
//    }
//
//    if (phaseId == Shenandoah.VALIDATE_RELEASE) {
//      validateTrace.release();
//      copy.reset();
//      super.collectionPhase(Shenandoah.RELEASE, primary);
//      return;
//    }


    if (phaseId == Shenandoah.CLEANUP) {
      if (!concurrentCleanupExecuted) return;
      atomicCounter.set(0);
      rendezvous();
      int index;
      while ((index = atomicCounter.add(1)) < Shenandoah.relocationSet.length()) {
        Address region = Shenandoah.relocationSet.get(index);
        Shenandoah.relocationSet.set(index, Address.zero());
        if (!region.isZero()) Shenandoah.regionSpace.release(region);
      }
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
    if (VM.VERIFY_ASSERTIONS) Log.writeln(Phase.getName(phaseId));
    if (phaseId == Shenandoah.CONCURRENT_CLOSURE) {
      currentTrace = TRACE_MARK;
      super.concurrentCollectionPhase(Shenandoah.CONCURRENT_CLOSURE);
      return;
    }

    if (phaseId == Shenandoah.CONCURRENT_FORWARD_CLOSURE) {
      currentTrace = TRACE_FORWARD;
      super.concurrentCollectionPhase(Shenandoah.CONCURRENT_CLOSURE);
      return;
    }

//    if (phaseId == Shenandoah.CONCURRENT_RELOCATION_SET_SELECTION) {
//      concurrentRelocationSetSelectionExecuted = true;
//      if (rendezvous() == 0) {
//        AddressArray blocksSnapshot = Shenandoah.regionSpace.snapshotRegions(false);
//        Shenandoah.relocationSet = RegionSpace.computeRelocationRegions(blocksSnapshot, false, false);
//        RegionSpace.markRegionsAsRelocate(Shenandoah.relocationSet);
//      }
//      notifyConcurrentPhaseEnd();
//      return;
//    }
//
//    if (phaseId == Shenandoah.CONCURRENT_EAGER_CLEANUP) {
//      concurrentEagerCleanupExecuted = true;
//      atomicCounter.set(0);
//      rendezvous();
//      int index;
//      while ((index = atomicCounter.add(1)) < Shenandoah.relocationSet.length()) {
//        Address region = Shenandoah.relocationSet.get(index);
//        if (!region.isZero() && Region.usedSize(region) == 0) {
//          Shenandoah.relocationSet.set(index, Address.zero());
//          Shenandoah.regionSpace.release(region);
//        }
//      }
//      notifyConcurrentPhaseEnd();
//      return;
//    }

    if (phaseId == Shenandoah.CONCURRENT_EVACUATE) {
      copy.reset();
      concurrentEvacuationExecuted = true;
      rendezvous();
      evacuateRegions(true);
      notifyConcurrentPhaseEnd();
      return;
    }

//    if (phaseId == Shenandoah.CONCURRENT_CLEANUP) {
//      concurrentCleanupExecuted = true;
//      atomicCounter.set(0);
//      rendezvous();
//      int index;
//      while ((index = atomicCounter.add(1)) < Shenandoah.relocationSet.length()) {
//        Address region = Shenandoah.relocationSet.get(index);
//        Shenandoah.relocationSet.set(index, Address.zero());
//        if (!region.isZero()) Shenandoah.regionSpace.release(region);
//      }
//      notifyConcurrentPhaseEnd();
//      return;
//    }
  }

  @NoInline
  @Unpreemptible
  private void notifyConcurrentPhaseEnd() {
    if (rendezvous() == 0) {
      continueCollecting = false;
      if (!group.isAborted()) {
        VM.collection.requestMutatorFlush();
        continueCollecting = Phase.notifyConcurrentPhaseComplete();
      }
    }
    rendezvous();
  }

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>RegionalCopy</code> instance. */
  @Inline
  private static Shenandoah global() {
    return (Shenandoah) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    switch (currentTrace) {
      case TRACE_MARK: return markTrace;
      case TRACE_EVACUATE: return evacuateTrace;
      case TRACE_FORWARD: return forwardTrace;
      case TRACE_VALIDATE: return validateTrace;
      default: {
        VM.assertions.fail("Unreachable");
        return null;
      }
    }
  }
}
