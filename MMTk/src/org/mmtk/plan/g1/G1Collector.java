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
import org.mmtk.policy.region.CardTable;
import org.mmtk.policy.region.Region;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.RegionAllocator2;
import org.mmtk.utility.deque.ObjectReferenceDeque;
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
public class G1Collector extends G1CollectorBase {

  protected static final int MARK_TRACE     = 0;
  protected static final int EVACUATE_TRACE = 1;
  protected static final int NURSERY_TRACE  = 2;
  protected static final int VALIDATE_TRACE = 3;

  protected final ObjectReferenceDeque modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
  protected final G1MarkTraceLocal markTrace = new G1MarkTraceLocal(global().markTrace, modbuf);
  protected final G1EvacuateTraceLocal evacuateTrace = new G1EvacuateTraceLocal(global().evacuateTrace);
  protected final G1NurseryTraceLocal nurseryTrace = new G1NurseryTraceLocal(global().nurseryTrace);
  protected final Validation.TraceLocal validateTrace = new Validation.TraceLocal();
  protected int currentTrace = MARK_TRACE;

  protected final RegionAllocator2 g1Survivor = new RegionAllocator2(G1.regionSpace, Region.SURVIVOR);
  protected final RegionAllocator2 g1Old = new RegionAllocator2(G1.regionSpace, Region.OLD);

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
    switch (allocator) {
      case G1.ALLOC_G1_SURVIVOR: return g1Survivor.alloc(bytes, align, offset);
      case G1.ALLOC_G1_OLD:      return g1Old.alloc(bytes, align, offset);
      default:
        VM.assertions.fail("Unreachable");
        return Address.zero();
    }
  }

  @Override
  @Inline
  public void postCopy(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    ForwardingWord.clearForwardingBits(object);
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

    if (phaseId == G1.REFINE_CARDS) {
      CardRefinement.collectorRefineAllDirtyCards(parallelWorkerOrdinal(), parallelWorkerCount());
      return;
    }

    if (phaseId == G1.REMSET_ROOTS) {
      G1.regionSpace.iterateToSpaceRemSetRoots(getCurrentTrace(), parallelWorkerOrdinal(), parallelWorkerCount(), G1.gcKind == G1.GCKind.YOUNG);
      return;
    }

    if (phaseId == G1.EVACUATE_PREPARE) {
      currentTrace = G1.gcKind == G1.GCKind.YOUNG ? NURSERY_TRACE : EVACUATE_TRACE;
      if (VM.VERIFY_ASSERTIONS) {
        if (!G1.ENABLE_GENERATIONAL_GC) VM.assertions._assert(G1.gcKind != G1.GCKind.YOUNG);
      }
      g1Survivor.adjustTLABSize();
      g1Old.adjustTLABSize();
      g1Survivor.reset();
      g1Old.reset();
      getCurrentTrace().prepare();
      super.collectionPhase(G1.PREPARE, primary);
      CardTable.clearAllHotnessPar(parallelWorkerOrdinal(), parallelWorkerCount());
      return;
    }

    if (phaseId == G1.EVACUATE_CLOSURE) {
      final int id = parallelWorkerOrdinal();
      long startTime = id == 0 ? VM.statistics.nanoTime() : 0;
      VM.activePlan.collector().rendezvous();
      getCurrentTrace().completeTrace();
      VM.activePlan.collector().rendezvous();
      if (id == 0) {
        G1.predictor.stat.totalCopyTime += VM.statistics.nanoTime() - startTime;
      }
      return;
    }

    if (phaseId == G1.EVACUATE_RELEASE) {
      getCurrentTrace().release();
      g1Survivor.reset();
      g1Old.reset();
      super.collectionPhase(G1.RELEASE, primary);
      return;
    }

    if (phaseId == Validation.VALIDATE_PREPARE) {
      currentTrace = VALIDATE_TRACE;
      validateTrace.prepare();
      return;
    }

    if (phaseId == Validation.VALIDATE_CLOSURE) {
      validateTrace.completeTrace();
      return;
    }

    if (phaseId == Validation.VALIDATE_RELEASE) {
      validateTrace.release();
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
    switch (currentTrace) {
      case MARK_TRACE:     return markTrace;
      case EVACUATE_TRACE: return evacuateTrace;
      case NURSERY_TRACE:  return nurseryTrace;
      case VALIDATE_TRACE: return validateTrace;
      default:
        VM.assertions.fail("unknown traceLocal");
        return null;
    }
  }
}
