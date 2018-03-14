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
package org.mmtk.plan.zgc;

import org.mmtk.plan.CollectorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldCollector;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.CopyLocal;
import org.mmtk.policy.LargeObjectLocal;
import org.mmtk.policy.Space;
import org.mmtk.policy.zgc.ZPage;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.ZAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements <i>per-collector thread</i> behavior
 * and state for the <i>ZGC</i> plan, which implements a full-heap
 * semi-space collector.<p>
 *
 * Specifically, this class defines <i>ZGC</i> collection behavior
 * (through <code>trace</code> and the <code>collectionPhase</code>
 * method), and collection-time allocation (copying of objects).<p>
 *
 * See {@link ZGC} for an overview of the semi-space algorithm.
 *
 * @see ZGC
 * @see ZGCMutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class ZGCCollector extends StopTheWorldCollector {

  /****************************************************************************
   * Instance fields
   */

  /**
   *
   */
  protected final ZAllocator copy = new ZAllocator(ZGC.zSpace, true);
  protected final ZGCTraceLocal markTrace = new ZGCTraceLocal(global().markTrace);
  protected final ZGCRelocationTraceLocal relocateTrace = new ZGCRelocationTraceLocal(global().relocateTrace);
  protected TraceLocal currentTrace;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public ZGCCollector() {}

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
      VM.assertions._assert(allocator == ZGC.ALLOC_DEFAULT);
    }
    return copy.alloc(bytes, align, offset);
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(allocator == ZGC.ALLOC_DEFAULT);

    ZGC.zSpace.postCopy(object, bytes);

    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(getCurrentTrace().isLive(object));
      if (!getCurrentTrace().willNotMoveInCurrentCollection(object)) {
        Log.writeln("#ZPage " + ZPage.of(object.toAddress()) + " is marked for relocate");
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
    if (phaseId == ZGC.PREPARE) {
      Log.writeln("ZGC PREPARE");
      currentTrace = markTrace;
      super.collectionPhase(phaseId, primary);
      markTrace.prepare();
      return;
    }

    if (phaseId == ZGC.CLOSURE) {
      Log.writeln("ZGC CLOSURE");
      markTrace.completeTrace();
      return;
    }

    if (phaseId == ZGC.RELEASE) {
      Log.writeln("ZGC RELEASE");
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == ZGC.RELOCATE_PREPARE) {
      Log.writeln("ZGC RELOCATE_PREPARE");
      currentTrace = relocateTrace;
      super.collectionPhase(ZGC.PREPARE, primary);
      relocateTrace.prepare();
      copy.reset();
      return;
    }

    if (phaseId == ZGC.RELOCATE_CLOSURE) {
      Log.writeln("ZGC RELOCATE_CLOSURE");
      relocateTrace.completeTrace();
      return;
    }

    if (phaseId == ZGC.RELOCATE_RELEASE) {
      Log.writeln("ZGC RELOCATE_RELEASE");
      relocateTrace.release();
      copy.reset();
      super.collectionPhase(ZGC.RELEASE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
    /*if (phaseId == ZGC.PREPARE) {
      copy.reset();
      trace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == ZGC.CLOSURE) {
      trace.completeTrace();
      return;
    }

    if (phaseId == ZGC.RELEASE) {
      trace.release();
      //zgc.release(true);
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
    return Space.isInSpace(ZGC.SS0, object) || Space.isInSpace(ZGC.SS1, object);
  }*/

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>ZGC</code> instance. */
  @Inline
  private static ZGC global() {
    return (ZGC) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
