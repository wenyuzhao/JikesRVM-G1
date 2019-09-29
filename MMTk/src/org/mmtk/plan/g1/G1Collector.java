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
package org.mmtk.plan.g1;

import org.mmtk.plan.*;
import org.mmtk.policy.region.Region;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.RegionAllocator2;
import org.mmtk.utility.deque.ObjectReferenceDeque;
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
public class G1Collector extends G1CollectorBase {

  protected static final byte MARK_TRACE = 0;
  protected static final byte EVACUATE_TRACE = 1;

  protected final ObjectReferenceDeque modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
  protected final RegionAllocator2 g1 = new RegionAllocator2(G1.regionSpace, Region.OLD);
  protected final G1MarkTraceLocal markTrace = new G1MarkTraceLocal(global().markTrace, modbuf);
  protected final G1EvacuateTraceLocal evacuateTrace = new G1EvacuateTraceLocal(global().evacuateTrace);
  protected byte currentTrace = MARK_TRACE;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public G1Collector() {
    super();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address allocCopy(ObjectReference original, int bytes, int align, int offset, int allocator) {
    return g1.alloc(bytes, align, offset);
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
//  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    if (G1.VERBOSE) Log.writeln(Phase.getName(phaseId));

    if (phaseId == G1.FLUSH_COLLECTOR) {
      if (G1.VERBOSE) Log.writeln(Phase.getName(phaseId));
      getCurrentTrace().processRoots();
      getCurrentTrace().flush();
      if (G1.VERBOSE) Log.writeln("flush-collector end");
      return;
    }

    if (phaseId == G1.FINAL_MARK) {
      if (G1.VERBOSE) Log.writeln(Phase.getName(phaseId));
      getCurrentTrace().completeTrace();
      return;
    }

    if (phaseId == G1.PREPARE) {
      currentTrace = MARK_TRACE;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.CLOSURE) {
      markTrace.completeTrace();
      return;
    }

    if (phaseId == G1.RELEASE) {
      markTrace.completeTrace();
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.EVACUATE_PREPARE) {
      currentTrace = EVACUATE_TRACE;
      evacuateTrace.prepare();
      g1.reset();
      super.collectionPhase(G1.PREPARE, primary);
      return;
    }

    if (phaseId == G1.EVACUATE_CLOSURE) {
      evacuateTrace.completeTrace();
      return;
    }

    if (phaseId == G1.EVACUATE_RELEASE) {
      evacuateTrace.release();
      g1.reset();
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

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace == MARK_TRACE ? markTrace : evacuateTrace;
  }
}
