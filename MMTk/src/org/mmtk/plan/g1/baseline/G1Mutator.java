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
  protected final RegionAllocator2 ra;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public G1Mutator() {
    ra = new RegionAllocator2(G1.regionSpace, Region.NORMAL);
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
    if (allocator == G1.ALLOC_G1) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return ra.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    if (allocator == G1.ALLOC_G1) {
//      Regional.regionSpace.initializeHeader(object, bytes);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == G1.regionSpace) return ra;
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
      ra.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.RELEASE) {
      ra.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.EVACUATE_PREPARE) {
      ra.reset();
      super.collectionPhase(G1.PREPARE, primary);
      return;
    }

    if (phaseId == G1.EVACUATE_RELEASE) {
      ra.reset();
      super.collectionPhase(G1.RELEASE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    ra.reset();
    assertRemsetsFlushed();
  }

  @Inline
  G1 global() {
    return (G1) VM.activePlan.global();
  }
}
