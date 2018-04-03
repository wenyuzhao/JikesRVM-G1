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
import org.mmtk.plan.concurrent.Concurrent;
import org.mmtk.policy.MarkBlockSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.options.DefragHeadroomFraction;
import org.mmtk.utility.options.Options;
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
public class MarkCopy extends Concurrent {

  /****************************************************************************
   *
   * Class variables
   */

  /** One of the two semi spaces that alternate roles at each collection */
  public static final MarkBlockSpace markBlockSpace = new MarkBlockSpace("rc", VMRequest.discontiguous());
  public static final int MC = markBlockSpace.getDescriptor();

  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace relocateTrace = new Trace(metaDataSpace);

  static {
    Options.defragHeadroomFraction = new DefragHeadroomFraction();
    Options.defragHeadroomFraction.setDefaultValue(0.05f);
    // msSpace.makeAllocAsMarked();
    smallCodeSpace.makeAllocAsMarked();
    nonMovingSpace.makeAllocAsMarked();
  }

  /**
   *
   */
  public static final int ALLOC_MC = Plan.ALLOC_DEFAULT;
  public static final int SCAN_MARK = 0;
  public static final int SCAN_RELOCATE = 1;

  /* Phases */
  public static final short RELOCATE_PREPARE = Phase.createSimple("relocate-prepare");
  public static final short RELOCATE_CLOSURE = Phase.createSimple("relocate-closure");
  public static final short RELOCATE_RELEASE = Phase.createSimple("relocate-release");

  public static final short CONCURRENT_RELOCATE_PREPARE = Phase.createConcurrent("concurrent-relocate-prepare-simple", Phase.scheduleCollector(RELOCATE_PREPARE));
  public static final short concurrentRelocatePrepare = Phase.createComplex("concurrent-relocate-prepare", null,
    Phase.scheduleConcurrent(CONCURRENT_RELOCATE_PREPARE),
    Phase.scheduleCollector(RELOCATE_PREPARE)
  );

  public static final short CONCURRENT_RELOCATE_RELEASE = Phase.createConcurrent("concurrent-relocate-release-simple", Phase.scheduleCollector(RELOCATE_RELEASE));

  /**
   * This is the phase that is executed to perform a mark-compact collection.
   *
   * FIXME: Far too much duplication and inside knowledge of StopTheWorld
   */
  public short _collection = Phase.createComplex("_collection", null,
      Phase.scheduleComplex  (initPhase),
      Phase.scheduleComplex  (rootClosurePhase),
      Phase.scheduleComplex  (refTypeClosurePhase),
      Phase.scheduleComplex  (completeClosurePhase),


      Phase.scheduleGlobal   (RELOCATE_PREPARE),
      //Phase.scheduleComplex(concurrentRelocatePrepare),
      Phase.scheduleCollector(RELOCATE_PREPARE),
      Phase.scheduleMutator  (PREPARE),
      Phase.scheduleCollector(STACK_ROOTS),
      Phase.scheduleCollector(ROOTS),
      Phase.scheduleGlobal   (ROOTS),

    Phase.scheduleComplex  (forwardPhase),


      Phase.scheduleCollector(RELOCATE_CLOSURE),
      Phase.scheduleMutator  (RELEASE),
      //Phase.scheduleConcurrent(CONCURRENT_RELOCATE_RELEASE),
      Phase.scheduleCollector(RELOCATE_RELEASE),
      Phase.scheduleGlobal   (RELOCATE_RELEASE),

      Phase.scheduleComplex  (finishPhase)
  );

  /**
   * Constructor
   */
  public MarkCopy() {
    collection = _collection;
  }

  @Override
  protected boolean concurrentCollectionRequired() {
    return !Phase.concurrentPhaseActive() &&
        ((getPagesReserved() * 100) / getTotalPages()) > 40;
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
  public void collectionPhase(short phaseId) {
    if (phaseId == PREPARE) {
      super.collectionPhase(phaseId);
      markTrace.prepareNonBlocking();
      markBlockSpace.prepare();
      return;
    }

    if (phaseId == RELEASE) {
      markTrace.release();
      markBlockSpace.release();
      super.collectionPhase(phaseId);
      return;
    }

    if (phaseId == RELOCATE_PREPARE) {
      super.collectionPhase(PREPARE);
      // inConcurrentCollection = false;
      relocateTrace.prepare();
      markBlockSpace.prepare();
      return;
    }
    if (phaseId == RELOCATE_RELEASE) {
      relocateTrace.release();
      markBlockSpace.release();
      super.collectionPhase(RELEASE);
      return;
    }

    super.collectionPhase(phaseId);
  }

  /****************************************************************************
   *
   * Accounting
   */

  @Override
  protected boolean collectionRequired(boolean spaceFull, Space space) {
    return super.collectionRequired(spaceFull, space) ||  ((getPagesReserved() * 100) / getTotalPages()) > 95;
  }

  /**
   * Return the number of pages reserved for copying.
   */
  @Override
  public final int getCollectionReserve() {
    // we must account for the number of pages required for copying,
    // which equals the number of semi-space pages reserved
    return markBlockSpace.getCollectionReserve() + super.getCollectionReserve(); // TODO: Fix this
  }

  /**
   * Return the number of pages reserved for use given the pending
   * allocation.  This is <i>exclusive of</i> space reserved for
   * copying.
   */
  @Override
  public int getPagesUsed() {
    return super.getPagesUsed() + markBlockSpace.reservedPages();
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
    if (Space.isInSpace(MC, object)) return false;
    return super.willNeverMove(object);
  }

  @Override
  @Interruptible
  protected void registerSpecializedMethods() {
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, MarkCopyMarkTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_RELOCATE, MarkCopyRelocationTraceLocal.class);
    super.registerSpecializedMethods();
  }
}
