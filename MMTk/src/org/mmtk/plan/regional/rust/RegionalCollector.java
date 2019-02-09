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
import org.mmtk.plan.ParallelCollector;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.CollectorContext;
import org.mmtk.policy.Region;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
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
 * See {@link Regional} for an overview of the semi-space algorithm.
 *
 * @see Regional
 * @see RegionalMutator
 * @see ParallelCollector
 * @see CollectorContext
 */
@Uninterruptible
public class RegionalCollector extends ParallelCollector {

  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator copy = new RegionAllocator(Regional.regionSpace, Region.NORMAL);
  protected final RegionalMarkTraceLocal markTrace = new RegionalMarkTraceLocal(global().markTrace);
  protected final RegionalEvacuateTraceLocal evacuateTrace = new RegionalEvacuateTraceLocal(global().evacuateTrace);
  protected TraceLocal currentTrace;
  private static final Atomic.Int atomicCounter = new Atomic.Int();

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public RegionalCollector() {}

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
    Address addr = copy.alloc(bytes, align, offset);
    return addr;
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    Regional.regionSpace.postCopy(object, bytes);
  }

  /****************************************************************************
   *
   * Collection
   */

  @Override
  public void collect() {
    Phase.beginNewPhaseStack(Phase.scheduleComplex(global().collection));
  }

  @Unpreemptible
  public final void concurrentCollect() {
    VM.assertions.fail("concurrentCollect called on StopTheWorld collector");
  }

  /**
   * Perform some concurrent collection work.
   *
   * @param phaseId The unique phase identifier
   */
  public void concurrentCollectionPhase(short phaseId) {
    VM.assertions.fail("concurrentCollectionPhase triggered on StopTheWorld collector");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    if (VM.VERIFY_ASSERTIONS) Log.writeln(Phase.getName(phaseId));
    if (phaseId == Regional.PREPARE) {
      currentTrace = markTrace;
      markTrace.prepare();
      return;
    }

    if (phaseId == Regional.STACK_ROOTS) {
      VM.scanning.computeThreadRoots(getCurrentTrace());
      return;
    }

    if (phaseId == Regional.ROOTS) {
      VM.scanning.computeGlobalRoots(getCurrentTrace());
      VM.scanning.computeStaticRoots(getCurrentTrace());
      if (Plan.SCAN_BOOT_IMAGE) {
        VM.scanning.computeBootImageRoots(getCurrentTrace());
      }
      return;
    }

    if (phaseId == Regional.SOFT_REFS) {
      if (primary) {
        if (!Options.noReferenceTypes.getValue()) {
          if (!Plan.isEmergencyCollection()) {
            VM.softReferences.scan(getCurrentTrace(),global().isCurrentGCNursery(),true);
          }
        }
      }
      return;
    }

    if (phaseId == Regional.WEAK_REFS) {
      if (primary) {
        if (Options.noReferenceTypes.getValue()) {
          VM.softReferences.clear();
          VM.weakReferences.clear();
        } else {
          VM.softReferences.scan(getCurrentTrace(),global().isCurrentGCNursery(), false);
          VM.weakReferences.scan(getCurrentTrace(),global().isCurrentGCNursery(), false);
        }
      }
      return;
    }

    if (phaseId == Regional.FINALIZABLE) {
      if (primary) {
        if (Options.noFinalizer.getValue())
          VM.finalizableProcessor.clear();
        else
          VM.finalizableProcessor.scan(getCurrentTrace(),global().isCurrentGCNursery());
      }
      return;
    }

    if (phaseId == Regional.PHANTOM_REFS) {
      if (primary) {
        if (Options.noReferenceTypes.getValue())
          VM.phantomReferences.clear();
        else
          VM.phantomReferences.scan(getCurrentTrace(),global().isCurrentGCNursery(),false);
      }
      return;
    }

    if (phaseId == Regional.FORWARD_REFS) {
      if (primary && !Options.noReferenceTypes.getValue() &&
          VM.activePlan.constraints().needsForwardAfterLiveness()) {
        VM.softReferences.forward(getCurrentTrace(),global().isCurrentGCNursery());
        VM.weakReferences.forward(getCurrentTrace(),global().isCurrentGCNursery());
        VM.phantomReferences.forward(getCurrentTrace(),global().isCurrentGCNursery());
      }
      return;
    }

    if (phaseId == Regional.FORWARD_FINALIZABLE) {
      if (primary && !Options.noFinalizer.getValue() &&
          VM.activePlan.constraints().needsForwardAfterLiveness()) {
        VM.finalizableProcessor.forward(getCurrentTrace(),global().isCurrentGCNursery());
      }
      return;
    }

    if (phaseId == Regional.CLOSURE) {
      markTrace.completeTrace();
      return;
    }

    if (phaseId == Regional.RELEASE) {
      markTrace.release();
      return;
    }

    if (phaseId == Regional.EAGER_CLEANUP) {
      atomicCounter.set(0);
      rendezvous();
      int index;
      while ((index = atomicCounter.add(1)) < Regional.relocationSet.length()) {
        Address region = Regional.relocationSet.get(index);
        if (!region.isZero() && Region.usedSize(region) == 0) {
          Regional.relocationSet.set(index, Address.zero());
          Regional.regionSpace.release(region);
        }
      }
      rendezvous();
      return;
    }

    if (phaseId == Regional.EVACUATE_PREPARE) {
      currentTrace = evacuateTrace;
      evacuateTrace.prepare();
      copy.reset();
      return;
    }

    if (phaseId == Regional.EVACUATE_CLOSURE) {
      evacuateTrace.completeTrace();
      return;
    }

    if (phaseId == Regional.EVACUATE_RELEASE) {
      evacuateTrace.release();
      copy.reset();
      return;
    }

    if (phaseId == Regional.CLEANUP_BLOCKS) {
      Regional.regionSpace.cleanupRegions(Regional.relocationSet, false);
      return;
    }

    if (phaseId == Regional.COMPLETE) {
      // Nothing to do
      return;
    }

//    if (Options.sanityCheck.getValue() && sanityLocal.collectionPhase(phaseId, primary)) {
//      return;
//    }

    Log.write("Per-collector phase ");
    Log.write(Phase.getName(phaseId));
    Log.writeln(" not handled.");
    VM.assertions.fail("Per-collector phase not handled!");
  }

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>RegionalCopy</code> instance. */
  @Inline
  private static Regional global() {
    return (Regional) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
