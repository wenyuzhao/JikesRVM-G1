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

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.plan.TraceWriteBuffer;
import org.mmtk.plan.concurrent.ConcurrentMutator;
import org.mmtk.policy.MarkRegionSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.MarkRegionAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements <i>per-mutator thread</i> behavior
 * and state for the <i>RegionalCopy</i> plan, which implements a full-heap
 * semi-space collector.<p>
 *
 * Specifically, this class defines <i>RegionalCopy</i> mutator-time allocation
 * and per-mutator thread collection semantics (flushing and restoring
 * per-mutator allocator state).<p>
 *
 * See {@link ConcMark} for an overview of the semi-space algorithm.
 *
 * @see ConcMark
 * @see ConcMarkCollector
 * @see StopTheWorldMutator
 * @see MutatorContext
 */
@Uninterruptible
public class ConcMarkMutator extends ConcurrentMutator {
  /****************************************************************************
   * Instance fields
   */
  protected final MarkRegionAllocator cm = new MarkRegionAllocator(ConcMark.markRegionSpace, false);
  private final TraceWriteBuffer remset = new TraceWriteBuffer(global().markTrace);

  /****************************************************************************
   *
   * Mutator-time allocation
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address alloc(int bytes, int align, int offset, int allocator, int site) {
    if (allocator == ConcMark.ALLOC_Z)
      return cm.alloc(bytes, align, offset);
    else
      return super.alloc(bytes, align, offset, allocator, site);
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (allocator == ConcMark.ALLOC_Z) {
      ConcMark.markRegionSpace.postAlloc(object, bytes);
      MarkRegionSpace.Header.mark(object);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == ConcMark.markRegionSpace) return cm;
    return super.getAllocatorFromSpace(space);
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
      super.collectionPhase(phaseId, primary);
      cm.reset();
      return;
    }

    if (phaseId == ConcMark.RELEASE) {
      cm.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (ref.isNull()) return;
    if (barrierActive) {
      if (!ref.isNull()) {
        if      (Space.isInSpace(ConcMark.CONC_MARK, ref)) ConcMark.markRegionSpace.traceMarkObject(remset, ref);
        else if (Space.isInSpace(ConcMark.IMMORTAL,   ref)) ConcMark.immortalSpace.traceObject(remset, ref);
        else if (Space.isInSpace(ConcMark.LOS,        ref)) ConcMark.loSpace.traceObject(remset, ref);
        else if (Space.isInSpace(ConcMark.NON_MOVING, ref)) ConcMark.nonMovingSpace.traceObject(remset, ref);
        else if (Space.isInSpace(ConcMark.SMALL_CODE, ref)) ConcMark.smallCodeSpace.traceObject(remset, ref);
        else if (Space.isInSpace(ConcMark.LARGE_CODE, ref)) ConcMark.largeCodeSpace.traceObject(remset, ref);
      }
    }
    if (VM.VERIFY_ASSERTIONS) {
      if (!ref.isNull() && !Plan.gcInProgress()) {
        if      (Space.isInSpace(ConcMark.CONC_MARK, ref)) VM.assertions._assert(ConcMark.markRegionSpace.isLive(ref));
        else if (Space.isInSpace(ConcMark.IMMORTAL,   ref)) VM.assertions._assert(ConcMark.immortalSpace.isLive(ref));
        else if (Space.isInSpace(ConcMark.LOS,        ref)) VM.assertions._assert(ConcMark.loSpace.isLive(ref));
        else if (Space.isInSpace(ConcMark.NON_MOVING, ref)) VM.assertions._assert(ConcMark.nonMovingSpace.isLive(ref));
        else if (Space.isInSpace(ConcMark.SMALL_CODE, ref)) VM.assertions._assert(ConcMark.smallCodeSpace.isLive(ref));
        else if (Space.isInSpace(ConcMark.LARGE_CODE, ref)) VM.assertions._assert(ConcMark.largeCodeSpace.isLive(ref));
      }
    }
  }

  @Inline
  private static ConcMark global() {
    return (ConcMark) VM.activePlan.global();
  }
}
