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
package org.mmtk.plan.markcopy.remset;

import org.mmtk.plan.*;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.MarkBlockSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.options.DefragHeadroomFraction;
import org.mmtk.utility.options.Options;
import org.mmtk.utility.sanitychecker.SanityChecker;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.AddressArray;
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
public class MarkCopy extends StopTheWorld {

  /****************************************************************************
   *
   * Class variables
   */

  /** One of the two semi spaces that alternate roles at each collection */
  public static final MarkBlockSpace markBlockSpace = new MarkBlockSpace("rc", VMRequest.discontiguous());
  public static final int MC = markBlockSpace.getDescriptor();

  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace relocateTrace = new Trace(metaDataSpace);
  public AddressArray blocksSnapshot;

  static {
    Options.defragHeadroomFraction = new DefragHeadroomFraction();
    Options.defragHeadroomFraction.setDefaultValue(0.05f);
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

  public static final short RELOCATION_SET_SELECTION_PREPARE = Phase.createSimple("relocation-set-selection-prepare");
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection");

  public static final short relocationSetSelection = Phase.createComplex("relocationSetSelection",
    Phase.scheduleGlobal(RELOCATION_SET_SELECTION_PREPARE),
    Phase.scheduleCollector(RELOCATION_SET_SELECTION_PREPARE),
    Phase.scheduleMutator(RELOCATION_SET_SELECTION_PREPARE),
    Phase.scheduleCollector(RELOCATION_SET_SELECTION)
  );

  public static final short relocationPhase = Phase.createComplex("relocation", null,
    Phase.scheduleGlobal   (RELOCATE_PREPARE),
    Phase.scheduleCollector(RELOCATE_PREPARE),
    Phase.scheduleMutator  (RELOCATE_PREPARE),


    Phase.scheduleMutator  (PREPARE_STACKS),
    Phase.scheduleGlobal   (PREPARE_STACKS),

    Phase.scheduleCollector(STACK_ROOTS),
    Phase.scheduleGlobal   (STACK_ROOTS),
    Phase.scheduleCollector(ROOTS),
    Phase.scheduleGlobal   (ROOTS),

    Phase.scheduleGlobal   (RELOCATE_CLOSURE),
    Phase.scheduleCollector(RELOCATE_CLOSURE),

    Phase.scheduleCollector(SOFT_REFS),
    Phase.scheduleGlobal   (RELOCATE_CLOSURE),
    Phase.scheduleCollector(RELOCATE_CLOSURE),

    Phase.scheduleCollector(WEAK_REFS),
    Phase.scheduleCollector(FINALIZABLE),
    Phase.scheduleGlobal   (RELOCATE_CLOSURE),
    Phase.scheduleCollector(RELOCATE_CLOSURE),

    Phase.scheduleCollector(PHANTOM_REFS),

    Phase.scheduleComplex  (forwardPhase),

    Phase.scheduleMutator  (RELOCATE_RELEASE),
    Phase.scheduleCollector(RELOCATE_RELEASE),
    Phase.scheduleGlobal   (RELOCATE_RELEASE)
  );

  public static final short CLEANUP_BLOCKS = Phase.createSimple("cleanup-blocks");

  public static short _collection = Phase.createComplex("_collection", null,
    Phase.scheduleComplex  (initPhase),
    Phase.scheduleComplex  (rootClosurePhase),
    Phase.scheduleComplex  (refTypeClosurePhase),
    Phase.scheduleComplex  (forwardPhase),
    Phase.scheduleComplex  (completeClosurePhase),

    Phase.scheduleComplex  (relocationSetSelection),

    Phase.scheduleComplex  (relocationPhase),

    Phase.scheduleCollector(CLEANUP_BLOCKS),

    Phase.scheduleComplex  (finishPhase)
  );

  /**
   * Constructor
   */
  public MarkCopy() {
    collection = _collection;
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
      markTrace.prepare();
      markBlockSpace.prepare(false);
      return;
    }
    if (phaseId == CLOSURE) {
      markTrace.prepare();
      return;
    }
    if (phaseId == RELEASE) {
      markTrace.release();
      markBlockSpace.release();
      super.collectionPhase(phaseId);
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION_PREPARE) {
      blocksSnapshot = markBlockSpace.shapshotBlocks();
      return;
    }

    if (phaseId == RELOCATE_PREPARE) {
      super.collectionPhase(PREPARE);
      relocateTrace.prepare();
      markBlockSpace.prepare(true);
      return;
    }
    if (phaseId == RELOCATE_CLOSURE) {
      relocateTrace.prepare();
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
    int totalBlocks = ((int) (getTotalPages() / EmbeddedMetaData.PAGES_IN_REGION)) * MarkBlock.BLOCKS_IN_REGION;
    int availBlocks = ((int) (getPagesAvail() / EmbeddedMetaData.PAGES_IN_REGION)) * MarkBlock.BLOCKS_IN_REGION;;
    if (availBlocks <= (int) (totalBlocks * Options.defragHeadroomFraction.getValue() + 0.5f)) {
      return true;
    }
    return super.collectionRequired(spaceFull, space);
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

  @Override
  public int sanityExpectedRC(ObjectReference object, int sanityRootRC) {
    Space space = Space.getSpaceForObject(object);

    // Nursery
    if (space == markBlockSpace) {
      // We are never sure about objects in MC.
      // This is not very satisfying but allows us to use the sanity checker to
      // detect dangling pointers.
      return SanityChecker.UNSURE;
    }
    return super.sanityExpectedRC(object, sanityRootRC);
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
