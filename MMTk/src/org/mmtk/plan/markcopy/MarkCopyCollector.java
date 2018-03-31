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
package org.mmtk.plan.markcopy;

import org.mmtk.plan.CollectorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldCollector;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.MarkBlock;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.MarkBlockAllocator;
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
 * See {@link MarkCopy} for an overview of the semi-space algorithm.
 *
 * @see MarkCopy
 * @see MarkCopyMutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class MarkCopyCollector extends StopTheWorldCollector {

  /****************************************************************************
   * Instance fields
   */

  /**
   *
   */
  protected final MarkBlockAllocator copy = new MarkBlockAllocator(MarkCopy.markBlockSpace, true);
  protected final MarkCopyMarkTraceLocal markTrace = new MarkCopyMarkTraceLocal(global().markTrace);
  protected final MarkCopyRelocationTraceLocal relocateTrace = new MarkCopyRelocationTraceLocal(global().relocateTrace);
  protected TraceLocal currentTrace;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public MarkCopyCollector() {}

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
      VM.assertions._assert(allocator == MarkCopy.ALLOC_DEFAULT);
    }
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= MarkBlock.BYTES_IN_BLOCK);
    //Log.write("AllocCopy ");
    Address addr = copy.alloc(bytes, align, offset);
    //Log.write(addr);
    //Log.write("..<", copy.cursor);
    Address region = MarkBlock.of(addr);
    //Log.writeln(" in region ", region);
    //Log.flush();
    if (VM.VERIFY_ASSERTIONS) {
      if (!region.isZero()) {
        VM.assertions._assert(MarkBlock.allocated(region));
        VM.assertions._assert(!MarkBlock.relocationRequired(region));
        VM.assertions._assert(MarkBlock.usedSize(region) == 0);
      } else {
        Log.writeln("ALLOCATED A NULL REGION");
      }
    }
    return addr;
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(allocator == MarkCopy.ALLOC_DEFAULT);

    /*if (VM.VERIFY_ASSERTIONS) {
      // VM.assertions._assert(getCurrentTrace().isLive(object));
      if (!getCurrentTrace().willNotMoveInCurrentCollection(object)) {
        Log.write("#Block ", MarkRegion.of(object.toAddress()));
        Log.write(" is marked for relocate:");
        Log.write(MarkRegion.relocationRequired(MarkRegion.of(object.toAddress())) ? "true" : "false");
        Log.write(" allocated:");
        Log.write(MarkRegion.allocated(MarkRegion.of(object.toAddress())) ? "true" : "false");
        Log.writeln(" used:", MarkRegion.usedSize(MarkRegion.of(object.toAddress())));
      }
      VM.assertions._assert(getCurrentTrace().willNotMoveInCurrentCollection(object));
    }*/

    MarkCopy.markBlockSpace.postCopy(object, bytes);

    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(getCurrentTrace().isLive(object));
      if (!getCurrentTrace().willNotMoveInCurrentCollection(object)) {
        Log.write("#Block ", MarkBlock.of(VM.objectModel.objectStartRef(object)));
        Log.write(" is marked for relocate:");
        Log.writeln(MarkBlock.relocationRequired(MarkBlock.of(VM.objectModel.objectStartRef(object))) ? "true" : "false");
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
    if (phaseId == MarkCopy.PREPARE) {
      Log.writeln("MarkCopy PREPARE");
      currentTrace = markTrace;
      super.collectionPhase(phaseId, primary);
      markTrace.prepare();
      return;
    }

    if (phaseId == MarkCopy.CLOSURE) {
      Log.writeln("MarkCopy CLOSURE");
      markTrace.completeTrace();
      return;
    }

    if (phaseId == MarkCopy.RELEASE) {
      Log.writeln("MarkCopy RELEASE");
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == MarkCopy.RELOCATE_PREPARE) {
      Log.writeln("MarkCopy RELOCATE_PREPARE");
      currentTrace = relocateTrace;
      super.collectionPhase(MarkCopy.PREPARE, primary);
      relocateTrace.prepare();
      copy.reset();
      return;
    }

    if (phaseId == MarkCopy.RELOCATE_CLOSURE) {
      Log.writeln("MarkCopy RELOCATE_CLOSURE");
      relocateTrace.completeTrace();
      return;
    }

    if (phaseId == MarkCopy.RELOCATE_RELEASE) {
      Log.writeln("MarkCopy RELOCATE_RELEASE");
      relocateTrace.release();
      copy.reset();
      super.collectionPhase(MarkCopy.RELEASE, primary);
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
  private static MarkCopy global() {
    return (MarkCopy) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
