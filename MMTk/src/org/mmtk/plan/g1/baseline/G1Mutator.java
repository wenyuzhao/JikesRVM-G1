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
package org.mmtk.plan.g1.baseline;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Phase;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.RegionAllocator2;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements <i>per-mutator thread</i> behavior
 * and state for the <i>RegionalCopy</i> plan, which implements a full-heap
 * semi-space collector.<p>
 *
 * Specifically, this class defines <i>RegionalCopy</i> mutator-time allocation
 * and per-mutator thread collection semantics (flushing and restoring
 * per-mutator allocator state).<p>
 *
 * See {@link G1} for an overview of the semi-space algorithm.
 *
 * @see G1
 * @see G1Collector
 * @see StopTheWorldMutator
 * @see MutatorContext
 */
@Uninterruptible
public class G1Mutator extends StopTheWorldMutator {

  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator2 g1;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public G1Mutator() {
    g1 = new RegionAllocator2(G1.regionSpace, Region.NORMAL);
  }

  /****************************************************************************
   *
   * Mutator-time allocation
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address alloc(int bytes, int align, int offset, int allocator, int site) {
    switch (allocator) {
      case G1.ALLOC_G1:  return g1.alloc(bytes, align, offset);
      case G1.ALLOC_LOS: return los.alloc(bytes, align, offset);
      default:           return immortal.alloc(bytes, align, offset);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference ref, ObjectReference typeRef, int bytes, int allocator) {
    switch (allocator) {
      case G1.ALLOC_G1:  return;
      case G1.ALLOC_LOS: G1.loSpace.initializeHeader(ref, true); return;
      default:           G1.immortalSpace.initializeHeader(ref);  return;
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == G1.regionSpace) return g1;
    return super.getAllocatorFromSpace(space);
  }

  /****************************************************************************
   *
   * Collection
   */

  /**
   * {@inheritDoc}
   */
  @Override
  public void collectionPhase(short phaseId, boolean primary) {
    if (G1.VERBOSE) {
      Log.write("Mutator ");
      Log.writeln(Phase.getName(phaseId));
    }
    if (phaseId == G1.PREPARE) {
      g1.adjustTLABSize();
      g1.reset();
      los.prepare(true);
      VM.memory.collectorPrepareVMSpace();
      return;
    }

    if (phaseId == G1.RELEASE) {
      g1.reset();
      los.release(true);
      VM.memory.collectorReleaseVMSpace();
      return;
    }

    if (phaseId == G1.EVACUATE_PREPARE) {
      g1.reset();
      los.prepare(true);
      VM.memory.collectorPrepareVMSpace();
      return;
    }

    if (phaseId == G1.EVACUATE_RELEASE) {
      g1.reset();
      los.release(true);
      VM.memory.collectorReleaseVMSpace();
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    g1.reset();
    assertRemsetsFlushed();
  }

  @Inline
  G1 global() {
    return (G1) VM.activePlan.global();
  }
}
