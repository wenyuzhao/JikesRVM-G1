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
import org.mmtk.policy.MarkSweepSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.vm.VM;

import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.ObjectReference;


/**
 * This class implements the global state of a a simple allocator
 * without a collector.
 */
@Uninterruptible
public class ZGC extends StopTheWorld {

  /*****************************************************************************
   * Class variables
   */

  /**
   *
   */
  public static final MarkSweepSpace msSpace = new MarkSweepSpace("ms", VMRequest.discontiguous());
  public static final int MARK_SWEEP = msSpace.getDescriptor();
  public static final int SCAN_MARK = 0;

  /*****************************************************************************
   * Instance variables
   */

  /**
   *
   */
  public final Trace msTrace = new Trace(metaDataSpace);


  /*****************************************************************************
   * Collection
   */

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public final void collectionPhase(short phaseId) {
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(false);
    if (phaseId == PREPARE) {
      super.collectionPhase(phaseId);
      msTrace.prepare();
      msSpace.prepare(true);
      return;
    }
    if (phaseId == CLOSURE) {
      msTrace.prepare();
      return;
    }
    if (phaseId == RELEASE) {
      msTrace.release();
      msSpace.release();
      super.collectionPhase(phaseId);
      return;
    }
    super.collectionPhase(phaseId);
  }

  /*****************************************************************************
   * Accounting
   */

  /**
   * {@inheritDoc}
   * The superclass accounts for its spaces, we just
   * augment this with the default space's contribution.
   */
  @Override
  public int getPagesUsed() {
    return (msSpace.reservedPages() + super.getPagesUsed());
  }


  /*****************************************************************************
   * Miscellaneous
   */

  /**
   * {@inheritDoc}
   */
  @Interruptible
  @Override
  protected void registerSpecializedMethods() {
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, ZGCTraceLocal.class);
    super.registerSpecializedMethods();
  }
  
  @Override
  public boolean willNeverMove(ObjectReference object) {
    if (Space.isInSpace(MARK_SWEEP, object))
      return true;
    return super.willNeverMove(object);
  }
}
