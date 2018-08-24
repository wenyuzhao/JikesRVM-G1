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

import org.mmtk.plan.CollectorContext;
import org.mmtk.plan.Phase;
import org.mmtk.plan.StopTheWorldCollector;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.concurrent.ConcurrentCollector;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.RegionAllocator;
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
  protected final RegionAllocator copy = new RegionAllocator(Shenandoah.regionSpace, Region.NORMAL);
  protected final ShenandoahMarkTraceLocal markTrace = new ShenandoahMarkTraceLocal(global().markTrace);
  protected final ShenandoahEvacuateTraceLocal evacuateTrace = new ShenandoahEvacuateTraceLocal(global().evacuateTrace);
  protected TraceLocal currentTrace;

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
    Shenandoah.regionSpace.initializeHeader(object);
  }

  /****************************************************************************
   *
   * Collection
   */

  private final EvacuationLinearScan evacuationLinearScan = new EvacuationLinearScan();
  private static final RegionSpace.RegionIterator regionIterator = Shenandoah.regionSpace.new RegionIterator();

  @Inline
  private void evacuateRegion(Address region) {
    Region.linearScan(evacuationLinearScan, region);
  }

  private static final Atomic.Int atomicCounter = new Atomic.Int();

  @Inline
  private void evacuateRegions() {
    atomicCounter.set(0);
    rendezvous();
    int index;
    while ((index = atomicCounter.add(1)) < Shenandoah.relocationSet.length()) {
      Log.writeln("Evacuating ", Shenandoah.relocationSet.get(index));
      evacuateRegion(Shenandoah.relocationSet.get(index));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    if (VM.VERIFY_ASSERTIONS) Log.writeln(Phase.getName(phaseId));
    if (phaseId == Shenandoah.PREPARE) {
      currentTrace = markTrace;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == Shenandoah.CLOSURE) {
      markTrace.completeTrace();
      return;
    }

    if (phaseId == Shenandoah.RELEASE) {
      Shenandoah.readBarrierEnabled = false;
      rendezvous();
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == Shenandoah.EVACUATE_PREPARE) {
      evacuateTrace.prepare();
      currentTrace = evacuateTrace;
      copy.reset();
      return;
    }

    if (phaseId == Shenandoah.EVACUATE) {
      Log.writeln("Enable barriers...");
      Shenandoah.readBarrierEnabled = true;
      rendezvous();
      evacuateRegions();
      rendezvous();
      return;
    }

    if (phaseId == Shenandoah.EVACUATE_RELEASE) {
      evacuateTrace.release();
      copy.reset();
      return;
    }

    if (phaseId == Shenandoah.CLEANUP_BLOCKS) {
      Shenandoah.regionSpace.cleanupBlocks(Shenandoah.relocationSet, false);
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
      currentTrace = markTrace;
    }
    super.concurrentCollectionPhase(phaseId);
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
    return currentTrace;
  }
}
