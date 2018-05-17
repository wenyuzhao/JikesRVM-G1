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
package org.mmtk.plan.concurrent.pureg1;

import org.mmtk.plan.*;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.MarkBlockSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.MarkBlockAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
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
 * See {@link PureG1} for an overview of the semi-space algorithm.
 *
 * @see PureG1
 * @see PureG1Mutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class PureG1Collector extends StopTheWorldCollector {

  /****************************************************************************
   * Instance fields
   */

  /**
   *
   */
  protected final MarkBlockAllocator copy = new MarkBlockAllocator(PureG1.markBlockSpace, true);
  protected final PureG1MarkTraceLocal markTrace = new PureG1MarkTraceLocal(global().markTrace);
  protected final PureG1RedirectTraceLocal redirectTrace = new PureG1RedirectTraceLocal(global().redirectTrace);
  protected final Validation validationTrace = new Validation();
  protected TraceLocal currentTrace;
  static AddressArray relocationSet;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public PureG1Collector() {}

  /****************************************************************************
   *
   * Collection-time allocation
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address allocCopy(ObjectReference original, int bytes, int align, int offset, int allocator) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(bytes <= Plan.MAX_NON_LOS_COPY_BYTES);
      VM.assertions._assert(allocator == PureG1.ALLOC_MC);
      VM.assertions._assert(ForwardingWord.stateIsBeingForwarded(VM.objectModel.readAvailableBitsWord(original)));
    }

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
  public void postCopy(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(allocator == PureG1.ALLOC_DEFAULT);

    MarkBlock.Card.updateCardMeta(object);
    PureG1.markBlockSpace.postCopy(object, bytes);

    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(getCurrentTrace().isLive(object));
      if (!getCurrentTrace().willNotMoveInCurrentCollection(object)) {
        Log.write("Block ", MarkBlock.of(VM.objectModel.objectStartRef(object)));
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
    if (phaseId == PureG1.PREPARE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 PREPARE");
      currentTrace = markTrace;
      markTrace.prepare();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == PureG1.CLOSURE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 CLOSURE");
      markTrace.completeTrace();
      return;
    }

    if (phaseId == PureG1.RELEASE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 RELEASE");
      markTrace.release();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == PureG1.RELOCATION_SET_SELECTION_PREPARE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 RELOCATION_SET_SELECTION_PREPARE");
      copy.reset();
      MarkBlockSpace.prepareComputeRelocationBlocks();
      return;
    }

    if (phaseId == PureG1.RELOCATION_SET_SELECTION) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 RELOCATION_SET_SELECTION");
      AddressArray relocationSet = MarkBlockSpace.computeRelocationBlocks(global().blocksSnapshot, false);
      if (relocationSet != null) {
        PureG1Collector.relocationSet = relocationSet;
      }
      return;
    }

    if (phaseId == PureG1.PREPARE_EVACUATION) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 PREPARE_EVACUATION");
      if (VM.activePlan.collector().getId() == 0) {
        VM.activePlan.resetMutatorIterator();
        PureG1Mutator m;
        while ((m = (PureG1Mutator) VM.activePlan.getNextMutator()) != null) {
          m.enqueueCurrentRSBuffer();
        }
        ConcurrentRemSetRefinement.refineAll();
        CardTable.assertAllCardsAreNotMarked();
      }
      rendezvous();
      return;
    }

    if (phaseId == PureG1.EVACUATION) {
      if (VM.VERIFY_ASSERTIONS) {
        Log.writeln("G1 EVACUATION");
        VM.assertions._assert(relocationSet != null);
      }
      RemSet.evacuateBlocks(relocationSet, false);
      return;
    }

    if (phaseId == PureG1.REDIRECT_PREPARE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 REDIRECT_PREPARE");
      currentTrace = redirectTrace;
      redirectTrace.prepare();
      copy.reset();
      super.collectionPhase(PureG1.PREPARE, primary);
      return;
    }

    if (phaseId == PureG1.REDIRECT_CLOSURE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 REDIRECT_CLOSURE");
      redirectTrace.completeTrace();
      //redirectTrace.processRoots();
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 REDIRECT_CLOSURE Done.");
      return;
    }

    if (phaseId == PureG1.REDIRECT_RELEASE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("G1 REDIRECT_RELEASE");
      redirectTrace.release();
      copy.reset();
      super.collectionPhase(PureG1.RELEASE, primary);
      MarkBlock.Card.clearCardMetaForUnmarkedCards(false);
      return;
    }

    if (phaseId == PureG1.CLEANUP_BLOCKS) {
      if (VM.VERIFY_ASSERTIONS) {
        Log.writeln("G1 CLEANUP_BLOCKS");
        VM.assertions._assert(relocationSet != null);
      }
      RemSet.clearRemsetForRelocationSet(relocationSet, false);
      PureG1.markBlockSpace.cleanupBlocks(relocationSet, false);
      rendezvous();
      return;
    }



    if (phaseId == Validation.PREPARE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("Validation PREPARE");
      currentTrace = validationTrace;
      validationTrace.prepare();
      copy.reset();
      super.collectionPhase(PureG1.PREPARE, primary);
      return;
    }

    if (phaseId == Validation.CLOSURE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("Validation CLOSURE");
      validationTrace.completeTrace();
      if (VM.VERIFY_ASSERTIONS) Log.writeln("Validation CLOSURE Done.");
      return;
    }

    if (phaseId == Validation.RELEASE) {
      if (VM.VERIFY_ASSERTIONS) Log.writeln("Validation RELEASE");
      validationTrace.release();
      copy.reset();
      super.collectionPhase(PureG1.RELEASE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>RegionalCopy</code> instance. */
  @Inline
  private static PureG1 global() {
    return (PureG1) VM.activePlan.global();
  }

  @Override
  public TraceLocal getCurrentTrace() {
    return currentTrace;
  }
}
