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
package org.mmtk.plan.regionalcopy;

import org.mmtk.plan.CollectorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldCollector;
import org.mmtk.plan.TraceLocal;
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
 * See {@link RegionalCopy} for an overview of the semi-space algorithm.
 *
 * @see RegionalCopy
 * @see RegionalCopyMutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class RegionalCopyCollector extends StopTheWorldCollector {

  /****************************************************************************
   * Instance fields
   */

  /**
   *
   */
  protected final MarkRegionAllocator copy = new MarkRegionAllocator(RegionalCopy.markRegionSpace, true);
  protected final RegionalCopyMarkTraceLocal markTrace = new RegionalCopyMarkTraceLocal(global().markTrace);
  protected final RegionalCopyRelocationTraceLocal relocateTrace = new RegionalCopyRelocationTraceLocal(global().relocateTrace);
  protected TraceLocal currentTrace;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public RegionalCopyCollector() {}

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
      VM.assertions._assert(allocator == RegionalCopy.ALLOC_DEFAULT);
    }
    return copy.alloc(bytes, align, offset);
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(allocator == RegionalCopy.ALLOC_DEFAULT);

    RegionalCopy.markRegionSpace.postCopy(object, bytes);

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
    if (phaseId == RegionalCopy.PREPARE) {
      Log.writeln("RegionalCopy PREPARE");
      currentTrace = markTrace;
      super.collectionPhase(phaseId, primary);
      markTrace.prepare();
      return;
    }

    if (phaseId == RegionalCopy.CLOSURE) {
      Log.writeln("RegionalCopy CLOSURE");
      markTrace.completeTrace();
      return;
    }

    if (phaseId == RegionalCopy.RELEASE) {
      Log.writeln("RegionalCopy RELEASE");
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == RegionalCopy.RELOCATE_PREPARE) {
      Log.writeln("RegionalCopy RELOCATE_PREPARE");
      currentTrace = relocateTrace;
      super.collectionPhase(RegionalCopy.PREPARE, primary);
      relocateTrace.prepare();
      copy.reset();
      return;
    }

    if (phaseId == RegionalCopy.RELOCATE_CLOSURE) {
      Log.writeln("RegionalCopy RELOCATE_CLOSURE");
      relocateTrace.completeTrace();
      return;
    }

    if (phaseId == RegionalCopy.RELOCATE_RELEASE) {
      Log.writeln("RegionalCopy RELOCATE_RELEASE");
      relocateTrace.release();
      copy.reset();
      super.collectionPhase(RegionalCopy.RELEASE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
    /*if (phaseId == RegionalCopy.PREPARE) {
      copy.reset();
      trace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == RegionalCopy.CLOSURE) {
      trace.completeTrace();
      return;
    }

    if (phaseId == RegionalCopy.RELEASE) {
      trace.release();
      //regionalcopy.release(true);
      super.collectionPhase(phaseId, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);*/
  }


  /****************************************************************************
   *
   * Object processing and tracing
   */

  /**
   * Return {@code true} if the given reference is to an object that is within
   * one of the semi-spaces.
   *
   * @param object The object in question
   * @return {@code true} if the given reference is to an object that is within
   * one of the semi-spaces.
   */
  /*public static boolean isSemiSpaceObject(ObjectReference object) {
    return Space.isInSpace(RegionalCopy.SS0, object) || Space.isInSpace(RegionalCopy.SS1, object);
  }*/

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>RegionalCopy</code> instance. */
  @Inline
  private static RegionalCopy global() {
    return (RegionalCopy) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
