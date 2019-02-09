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
import org.mmtk.plan.concurrent.ConcurrentCollector;
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
 * See {@link G1} for an overview of the semi-space algorithm.
 *
 * @see G1
 * @see G1Mutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class G1Collector extends StopTheWorldCollector {

  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator copy = new RegionAllocator(G1.regionSpace, Region.NORMAL);
  protected final G1MarkTraceLocal markTrace = new G1MarkTraceLocal(global().markTrace);
  protected final G1EvacuateTraceLocal evacuateTrace = new G1EvacuateTraceLocal(global().evacuateTrace);
  protected TraceLocal currentTrace;
  private static final Atomic.Int atomicCounter = new Atomic.Int();

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public G1Collector() {}

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
    G1.regionSpace.postCopy(object, bytes);
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
    if (phaseId == G1.PREPARE) {
      currentTrace = markTrace;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.CLOSURE) {
      markTrace.completeTrace();
      return;
    }

    if (phaseId == G1.RELEASE) {
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.EAGER_CLEANUP) {
      atomicCounter.set(0);
      rendezvous();
      int index;
      while ((index = atomicCounter.add(1)) < G1.relocationSet.length()) {
        Address region = G1.relocationSet.get(index);
        if (!region.isZero() && Region.usedSize(region) == 0) {
          G1.relocationSet.set(index, Address.zero());
          G1.regionSpace.release(region);
        }
      }
      rendezvous();
      return;
    }

    if (phaseId == G1.EVACUATE_PREPARE) {
      currentTrace = evacuateTrace;
      evacuateTrace.prepare();
      copy.reset();
      super.collectionPhase(G1.PREPARE, primary);
      return;
    }

    if (phaseId == G1.EVACUATE_CLOSURE) {
      evacuateTrace.completeTrace();
      return;
    }

    if (phaseId == G1.EVACUATE_RELEASE) {
      evacuateTrace.release();
      copy.reset();
      super.collectionPhase(G1.RELEASE, primary);
      return;
    }

    if (phaseId == G1.CLEANUP_BLOCKS) {
      G1.regionSpace.cleanupRegions(G1.relocationSet, false);
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
  private static G1 global() {
    return (G1) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
