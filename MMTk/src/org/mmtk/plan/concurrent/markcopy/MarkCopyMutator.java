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
package org.mmtk.plan.concurrent.markcopy;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.plan.TraceWriteBuffer;
import org.mmtk.plan.concurrent.ConcurrentMutator;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.Space;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.MarkBlockAllocator;
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
 * See {@link MarkCopy} for an overview of the semi-space algorithm.
 *
 * @see MarkCopy
 * @see MarkCopyCollector
 * @see StopTheWorldMutator
 * @see MutatorContext
 */
@Uninterruptible
public class MarkCopyMutator extends ConcurrentMutator {
  /****************************************************************************
   * Instance fields
   */
  protected final MarkBlockAllocator mc;
  private final TraceWriteBuffer remset;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public MarkCopyMutator() {
    mc = new MarkBlockAllocator(MarkCopy.markBlockSpace, false);
    remset = new TraceWriteBuffer(global().markTrace);
  }

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
    if (allocator == MarkCopy.ALLOC_MC) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= MarkBlock.BYTES_IN_BLOCK);
      return mc.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (allocator == MarkCopy.ALLOC_MC) {
      MarkCopy.markBlockSpace.postAlloc(object, bytes);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == MarkCopy.markBlockSpace) return mc;
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
    if (phaseId == MarkCopy.PREPARE) {
      super.collectionPhase(phaseId, primary);
      mc.reset();
      return;
    }

    if (phaseId == MarkCopy.RELEASE) {
      mc.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (ref.isNull()) return;
    //if (barrierActive) {
    ObjectReference newRef = null;
    if (Space.isInSpace(MarkCopy.MC, ref)) newRef = MarkCopy.markBlockSpace.traceMarkObject(remset, ref);
    else if (Space.isInSpace(MarkCopy.IMMORTAL, ref)) newRef = MarkCopy.immortalSpace.traceObject(remset, ref);
    else if (Space.isInSpace(MarkCopy.LOS, ref)) newRef = MarkCopy.loSpace.traceObject(remset, ref);
    else if (Space.isInSpace(MarkCopy.NON_MOVING, ref)) newRef = MarkCopy.nonMovingSpace.traceObject(remset, ref);
    else if (Space.isInSpace(MarkCopy.SMALL_CODE, ref)) newRef = MarkCopy.smallCodeSpace.traceObject(remset, ref);
    else if (Space.isInSpace(MarkCopy.LARGE_CODE, ref)) newRef = MarkCopy.largeCodeSpace.traceObject(remset, ref);
    //}

    if (VM.VERIFY_ASSERTIONS) {
      if (!newRef.isNull() && !Plan.gcInProgress()) {
        if (Space.isInSpace(MarkCopy.MC, newRef)) VM.assertions._assert(MarkCopy.markBlockSpace.isLive(newRef));
        else if (Space.isInSpace(MarkCopy.IMMORTAL, newRef)) VM.assertions._assert(MarkCopy.immortalSpace.isLive(newRef));
        else if (Space.isInSpace(MarkCopy.LOS, newRef)) VM.assertions._assert(MarkCopy.loSpace.isLive(newRef));
        else if (Space.isInSpace(MarkCopy.NON_MOVING, newRef)) VM.assertions._assert(MarkCopy.nonMovingSpace.isLive(newRef));
        else if (Space.isInSpace(MarkCopy.SMALL_CODE, newRef)) VM.assertions._assert(MarkCopy.smallCodeSpace.isLive(newRef));
        else if (Space.isInSpace(MarkCopy.LARGE_CODE, newRef)) VM.assertions._assert(MarkCopy.largeCodeSpace.isLive(newRef));
      }
    }
  }

  @Override
  public void flushRememberedSets() {
    remset.flush();
    mc.reset();
  }

  @Inline
  private static MarkCopy global() {
    return (MarkCopy) VM.activePlan.global();
  }
}
