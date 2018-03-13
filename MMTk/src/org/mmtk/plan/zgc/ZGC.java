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

import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorld;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.CopySpace;
import org.mmtk.policy.Space;
import org.mmtk.policy.zgc.ZSpace;
import org.mmtk.utility.Log;
import org.mmtk.utility.heap.VMRequest;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements a simple semi-space collector. See the Jones
 * &amp; Lins GC book, section 2.2 for an overview of the basic
 * algorithm. This implementation also includes a large object space
 * (LOS), and an uncollected "immortal" space.<p>
 *
 * All plans make a clear distinction between <i>global</i> and
 * <i>thread-local</i> activities.  Global activities must be
 * synchronized, whereas no synchronization is required for
 * thread-local activities.  Instances of Plan map 1:1 to "kernel
 * threads" (aka CPUs).  Thus instance
 * methods allow fast, unsychronized access to Plan utilities such as
 * allocation and collection.  Each instance rests on static resources
 * (such as memory and virtual memory resources) which are "global"
 * and therefore "static" members of Plan.  This mapping of threads to
 * instances is crucial to understanding the correctness and
 * performance properties of this plan.
 */
@Uninterruptible
public class ZGC extends StopTheWorld {

  /****************************************************************************
   *
   * Class variables
   */

  /** One of the two semi spaces that alternate roles at each collection */
  public static final ZSpace zSpace = new ZSpace("z", false, VMRequest.discontiguous());
  public static final int Z = zSpace.getDescriptor();

  public final Trace zTrace = new Trace(metaDataSpace);

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Class variables
   */

  /**
   *
   */
  public static final int ALLOC_Z = Plan.ALLOC_DEFAULT;
  public static final int SCAN_MARK = 0;
  public static final int SCAN_RELOCATE = 1;

  /**
   * Constructor
   */
  public ZGC() {}

  /****************************************************************************
   *
   * Collection
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId) {
    if (phaseId == SET_COLLECTION_KIND) {
      super.collectionPhase(phaseId);
      return;
    }

    if (phaseId == ZGC.PREPARE) {
      Log.writeln("ZGC Prepare");
      super.collectionPhase(phaseId);
      zSpace.prepare();
      zTrace.prepare();
      return;
    }

    if (phaseId == CLOSURE) {
      Log.writeln("ZGC Closure");
      zTrace.prepare();
      return;
    }

    if (phaseId == ZGC.RELEASE) {
      Log.writeln("ZGC Release");
      zTrace.release();
      zSpace.release();
      super.collectionPhase(phaseId);
      return;
    }

    super.collectionPhase(phaseId);
  }

  /****************************************************************************
   *
   * Accounting
   */

  /**
   * Return the number of pages reserved for copying.
   */
  @Override
  public final int getCollectionReserve() {
    // we must account for the number of pages required for copying,
    // which equals the number of semi-space pages reserved
    return (zSpace.reservedPages() > 0 ? 1 : 0) + super.getCollectionReserve(); // TODO: Fix this
  }

  /**
   * Return the number of pages reserved for use given the pending
   * allocation.  This is <i>exclusive of</i> space reserved for
   * copying.
   */
  @Override
  public int getPagesUsed() {
    return super.getPagesUsed() + (zSpace.reservedPages() > 0 ? zSpace.reservedPages() - 1 : 0);
  }

  /**
   * Return the number of pages available for allocation, <i>assuming
   * all future allocation is to the semi-space</i>.
   *
   * @return The number of pages available for allocation, <i>assuming
   * all future allocation is to the semi-space</i>.
   */
  // @Override public final int getPagesAvail() { return(super.getPagesAvail()) >> 1; }

  @Override
  public boolean willNeverMove(ObjectReference object) {
    if (Space.isInSpace(Z, object)) return true;
    return super.willNeverMove(object);
  }

  @Override
  @Interruptible
  protected void registerSpecializedMethods() {
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, ZGCTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_RELOCATE, ZGCRelocationTraceLocal.class);
    super.registerSpecializedMethods();
  }
}
