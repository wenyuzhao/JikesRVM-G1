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

import org.mmtk.plan.*;

import org.mmtk.vm.VM;

import org.vmmagic.pragma.*;

/**
 * This class implements <i>per-collector thread</i> behavior and state
 * for the <i>NoGC</i> plan, which simply allocates (without ever collecting
 * until the available space is exhausted.<p>
 *
 * Specifically, this class <i>would</i> define <i>NoGC</i> collection time semantics,
 * however, since this plan never collects, this class consists only of stubs which
 * may be useful as a template for implementing a basic collector.
 *
 * @see ZGC
 * @see ZGCMutator
 * @see CollectorContext
 */
@Uninterruptible
public class ZGCCollector extends StopTheWorldCollector {

  /************************************************************************
   * Instance fields
   */

  /**
   *
   */
  private final ZGCTraceLocal trace = new ZGCTraceLocal(global().msTrace, null);
  protected final TraceLocal currentTrace = trace;


  /****************************************************************************
   * Collection
   */

  /**
   * Perform a garbage collection
   */
  /*@Override
  public final void collect() {
    VM.assertions.fail("GC Triggered in NoGC Plan. Is -X:gc:ignoreSystemGC=true ?");
  }*/

  @Inline
  @Override
  public final void collectionPhase(short phaseId, boolean primary) {
    //VM.assertions.fail("GC Triggered in NoGC Plan.");
    if (phaseId == ZGC.PREPARE) {
      super.collectionPhase(phaseId, primary);
      trace.prepare();
      return;
    }

    if (phaseId == ZGC.CLOSURE) {
      trace.completeTrace();
      return;
    }

    if (phaseId == ZGC.RELEASE) {
      trace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  /****************************************************************************
   * Miscellaneous
   */

  /** @return The active global plan as a <code>NoGC</code> instance. */
  @Inline
  private static ZGC global() {
    return (ZGC) VM.activePlan.global();
  }

  @Override
  public final TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
