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
package org.mmtk.plan.regional.linearevacuation;

import org.mmtk.plan.CollectorContext;
import org.mmtk.plan.Phase;
import org.mmtk.plan.StopTheWorldCollector;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Region;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
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
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class RegionalCollector extends StopTheWorldCollector {

  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator copy = new RegionAllocator(Regional.regionSpace, Region.OLD);
  protected final RegionalMarkTraceLocal markTrace = new RegionalMarkTraceLocal(global().markTrace);
  protected final RegionalForwardTraceLocal forwardTrace = new RegionalForwardTraceLocal(global().forwardTrace);
  protected final EvacuationLinearScan evacuationLinearScan = new EvacuationLinearScan();
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

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    if (Region.verbose()) Log.writeln(Phase.getName(phaseId));
    if (phaseId == Regional.PREPARE) {
      currentTrace = markTrace;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == Regional.CLOSURE) {
      markTrace.completeTrace();
      return;
    }

    if (phaseId == Regional.RELEASE) {
      markTrace.release();
      super.collectionPhase(phaseId, primary);
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

    if (phaseId == Regional.EVACUATE) {
      evacuationLinearScan.evacuateRegions();
      return;
    }

    if (phaseId == Regional.FORWARD_PREPARE) {
      currentTrace = forwardTrace;
      forwardTrace.prepare();
      copy.reset();
      super.collectionPhase(Regional.PREPARE, primary);
      return;
    }

    if (phaseId == Regional.FORWARD_CLOSURE) {
      forwardTrace.completeTrace();
      return;
    }

    if (phaseId == Regional.FORWARD_RELEASE) {
      forwardTrace.release();
      copy.reset();
      super.collectionPhase(Regional.RELEASE, primary);
      if (primary) RegionAllocator.adjustTLABSize();
      return;
    }

    if (phaseId == Regional.CLEANUP_BLOCKS) {
      Regional.regionSpace.cleanupRegions(Regional.relocationSet, false);
      return;
    }

    super.collectionPhase(phaseId, primary);
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
