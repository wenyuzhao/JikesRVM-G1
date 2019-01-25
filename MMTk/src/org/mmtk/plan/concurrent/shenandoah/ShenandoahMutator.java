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

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.plan.TraceWriteBuffer;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.RegionAllocator;
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
 * See {@link Shenandoah} for an overview of the semi-space algorithm.
 *
 * @see Shenandoah
 * @see ShenandoahCollector
 * @see StopTheWorldMutator
 * @see MutatorContext
 */
@Uninterruptible
public class ShenandoahMutator extends ShenandoahMutatorBarriers {

  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator ra;
  private final TraceWriteBuffer markRemset, forwardRemset;
  private static final boolean REMSET_MARK = false;
  private static final boolean REMSET_FORWARD = true;
  private boolean currentRemSet = REMSET_MARK;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public ShenandoahMutator() {
    ra = new RegionAllocator(Shenandoah.regionSpace, Region.NORMAL);
    markRemset = new TraceWriteBuffer(global().markTrace);
    forwardRemset = new TraceWriteBuffer(global().forwardTrace);
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
    if (allocator == Shenandoah.ALLOC_RS) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return ra.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    if (allocator == Shenandoah.ALLOC_RS) {
      Shenandoah.regionSpace.initializeHeader(object, bytes);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
    Shenandoah.initializeIndirectionPointer(object);
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == Shenandoah.regionSpace) return ra;
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
    //Log.write("[Mutator] ");
    //Log.writeln(Phase.getName(phaseId));
    if (phaseId == Shenandoah.PREPARE) {
      ra.reset();
      currentRemSet = REMSET_MARK;
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == Shenandoah.RELEASE) {
      ra.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == Shenandoah.FORWARD_PREPARE) {
      ra.reset();
      currentRemSet = REMSET_FORWARD;
      super.collectionPhase(Shenandoah.PREPARE, primary);
      return;
    }

    if (phaseId == Shenandoah.FORWARD_RELEASE) {
      ra.reset();
      super.collectionPhase(Shenandoah.RELEASE, primary);
      return;
    }

//    if (phaseId == Shenandoah.VALIDATE_PREPARE) {
//      ra.reset();
//      super.collectionPhase(Shenandoah.PREPARE, primary);
//      return;
//    }
//
//    if (phaseId == Shenandoah.VALIDATE_RELEASE) {
//      ra.reset();
//      super.collectionPhase(Shenandoah.RELEASE, primary);
//      return;
//    }



//    if (phaseId == Shenandoah.SET_BARRIER_ACTIVE) {
//      referenceUpdatingBarrierActive = true;
//      super.collectionPhase(phaseId, primary);
//      return;
//    }
//
//    if (phaseId == Shenandoah.CLEAR_BARRIER_ACTIVE) {
//      referenceUpdatingBarrierActive = false;
//      super.collectionPhase(phaseId, primary);
//      return;
//    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    markRemset.flush();
    forwardRemset.flush();
    ra.reset();
    assertRemsetsFlushed();
  }

  @Override
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (ref.isNull()) return;
    TraceWriteBuffer remset = currentRemSet == REMSET_MARK ? markRemset : forwardRemset;
    if (Space.isInSpace(Shenandoah.RS, ref)) {
      if (currentRemSet == REMSET_MARK)
        Shenandoah.regionSpace.traceMarkObject(remset, ref);
      else
        Shenandoah.regionSpace.traceForwardObject(remset, ref);
    }
    else if (Space.isInSpace(Shenandoah.IMMORTAL, ref)) Shenandoah.immortalSpace.traceObject(remset, ref);
    else if (Space.isInSpace(Shenandoah.LOS, ref)) Shenandoah.loSpace.traceObject(remset, ref);
    else if (Space.isInSpace(Shenandoah.NON_MOVING, ref)) Shenandoah.nonMovingSpace.traceObject(remset, ref);
    else if (Space.isInSpace(Shenandoah.SMALL_CODE, ref)) Shenandoah.smallCodeSpace.traceObject(remset, ref);
    else if (Space.isInSpace(Shenandoah.LARGE_CODE, ref)) Shenandoah.largeCodeSpace.traceObject(remset, ref);
  }

  @Override
  public final void assertRemsetsFlushed() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(markRemset.isFlushed());
      VM.assertions._assert(forwardRemset.isFlushed());
    }
  }

  @Inline
  Shenandoah global() {
    return (Shenandoah) VM.activePlan.global();
  }
}
