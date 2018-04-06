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

import org.mmtk.plan.*;
import org.mmtk.plan.concurrent.ConcurrentCollector;
import org.mmtk.policy.MarkBlock;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.MarkBlockAllocator;
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
 * See {@link MarkCopy} for an overview of the semi-space algorithm.
 *
 * @see MarkCopy
 * @see MarkCopyMutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class MarkCopyCollector extends ConcurrentCollector {

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
      VM.assertions._assert(allocator == MarkCopy.ALLOC_MC);
      VM.assertions._assert(ForwardingWord.stateIsBeingForwarded(VM.objectModel.readAvailableBitsWord(original)));
    }
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= MarkBlock.BYTES_IN_BLOCK);

    Address addr = copy.alloc(bytes, align, offset);
    org.mmtk.utility.Memory.assertIsZeroed(addr, bytes);
    if (VM.VERIFY_ASSERTIONS) {
      Address region = MarkBlock.of(addr);
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
  public void postCopy(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(allocator == MarkCopy.ALLOC_DEFAULT);
    }

    MarkCopy.markBlockSpace.postCopy(object, bytes);

    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!MarkBlock.relocationRequired(MarkBlock.of(VM.objectModel.objectStartRef(object))));
      VM.assertions._assert(getCurrentTrace().isLive(object));
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
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy PREPARE");
      currentTrace = markTrace;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == MarkCopy.CLOSURE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy CLOSURE");
      markTrace.completeTrace();
      return;
    }

    if (phaseId == MarkCopy.RELEASE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy RELEASE");
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == MarkCopy.RELOCATE_PREPARE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy RELOCATE_PREPARE");
      currentTrace = relocateTrace;
      relocateTrace.prepare();
      copy.reset();
      super.collectionPhase(MarkCopy.PREPARE, primary);
      return;
    }

    if (phaseId == MarkCopy.RELOCATE_CLOSURE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy RELOCATE_CLOSURE");
      relocateTrace.completeTrace();
      return;
    }

    if (phaseId == MarkCopy.RELOCATE_RELEASE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy RELOCATE_RELEASE");
      relocateTrace.release();
      copy.reset();
      super.collectionPhase(MarkCopy.RELEASE, primary);
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
    if (phaseId == MarkCopy.CONCURRENT_CLOSURE) {
      currentTrace = markTrace;
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy CONCURRENT_CLOSURE");
      super.concurrentCollectionPhase(phaseId);
      return;
    }

    if (phaseId == MarkCopy.CONCURRENT_RELOCATE_PREPARE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy CONCURRENT_RELOCATE_PREPARE");
      currentTrace = relocateTrace;
      relocateTrace.prepare();
      copy.reset();
      if (rendezvous() == 0) {
        if (!group.isAborted()) {
          VM.collection.requestMutatorFlush();
          continueCollecting = Phase.notifyConcurrentPhaseComplete();
        }
      }
      rendezvous();
      return;
    }
    if (phaseId == MarkCopy.CONCURRENT_RELOCATE_RELEASE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("MarkCopy CONCURRENT_RELOCATE_RELEASE");
      relocateTrace.release();
      copy.reset();
      if (rendezvous() == 0) {
        if (!group.isAborted()) {
          VM.collection.requestMutatorFlush();
          continueCollecting = Phase.notifyConcurrentPhaseComplete();
        }
      }
      rendezvous();
      return;
    }
    super.concurrentCollectionPhase(phaseId);
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
  private static MarkCopy global() {
    return (MarkCopy) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(markTrace != null);
      VM.assertions._assert(relocateTrace != null);
      VM.assertions._assert(currentTrace != null);
    }
    return currentTrace;
  }
}
