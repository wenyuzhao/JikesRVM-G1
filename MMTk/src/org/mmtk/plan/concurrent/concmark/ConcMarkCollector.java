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
package org.mmtk.plan.concurrent.concmark;

import org.mmtk.plan.*;
import org.mmtk.plan.concurrent.Concurrent;
import org.mmtk.plan.concurrent.ConcurrentCollector;
import org.mmtk.policy.MarkRegion;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.MarkRegionAllocator;
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
 * See {@link ConcMark} for an overview of the semi-space algorithm.
 *
 * @see ConcMark
 * @see ConcMarkMutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class ConcMarkCollector extends ConcurrentCollector {

  /****************************************************************************
   * Instance fields
   */

  /**
   *
   */
  protected final MarkRegionAllocator copy = new MarkRegionAllocator(ConcMark.markRegionSpace, true);
  protected final ConcMarkMarkTraceLocal markTrace = new ConcMarkMarkTraceLocal(global().markTrace);
  protected final ConcMarkRelocationTraceLocal relocateTrace = new ConcMarkRelocationTraceLocal(global().relocateTrace);
  protected TraceLocal currentTrace = markTrace;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public ConcMarkCollector() {}

  /****************************************************************************
   *
   * Collection-time allocation
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address allocCopy(ObjectReference original, int bytes,
      int align, int offset, int allocator) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(bytes <= Plan.MAX_NON_LOS_COPY_BYTES);
      VM.assertions._assert(allocator == ConcMark.ALLOC_DEFAULT);
    }
    return copy.alloc(bytes, align, offset);
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(allocator == ConcMark.ALLOC_DEFAULT);

    ConcMark.markRegionSpace.postCopy(object, bytes);

    if (VM.VERIFY_ASSERTIONS) {
      // VM.assertions._assert(getCurrentTrace().isLive(object));
      if (!getCurrentTrace().willNotMoveInCurrentCollection(object)) {
        Log.writeln("#Block " + MarkRegion.of(object.toAddress()) + " is marked for relocate");
      }
      VM.assertions._assert(getCurrentTrace().willNotMoveInCurrentCollection(object));
    }
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
    if (phaseId == ConcMark.PREPARE) {
      Log.writeln("CM PREPARE");
      currentTrace = markTrace;
      super.collectionPhase(phaseId, primary);
      markTrace.prepare();
      return;
    }

    if (phaseId == ConcMark.CLOSURE) {
      Log.writeln("CM CLOSURE");
      markTrace.completeTrace();
      return;
    }

    if (phaseId == ConcMark.RELEASE) {
      Log.writeln("CM RELEASE");
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == ConcMark.RELOCATE_PREPARE) {
      Log.writeln("CM RELOCATE_PREPARE");
      currentTrace = relocateTrace;
      relocateTrace.prepare();
      copy.reset();
      return;
    }

    if (phaseId == ConcMark.RELOCATE_CLOSURE) {
      Log.writeln("CM RELOCATE_CLOSURE");
      relocateTrace.completeTrace();
      return;
    }

    if (phaseId == ConcMark.RELOCATE_RELEASE) {
      Log.writeln("CM RELOCATE_RELEASE");
      relocateTrace.release();
      copy.reset();
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

  /****************************************************************************
   *
   * Object processing and tracing
   */

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>RegionalCopy</code> instance. */
  @Inline
  private static ConcMark global() {
    return (ConcMark) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
