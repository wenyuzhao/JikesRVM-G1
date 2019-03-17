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
package org.mmtk.plan.regional;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Phase;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.RegionAllocator;
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
 * See {@link Regional} for an overview of the semi-space algorithm.
 *
 * @see Regional
 * @see RegionalCollector
 * @see StopTheWorldMutator
 * @see MutatorContext
 */
@Uninterruptible
public class RegionalMutator extends StopTheWorldMutator {

  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator ra;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public RegionalMutator() {
    ra = new RegionAllocator(Regional.regionSpace, Region.NORMAL);
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
    if (allocator == Regional.ALLOC_MC) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return ra.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    if (allocator == Regional.ALLOC_MC) {
//      Regional.regionSpace.initializeHeader(object, bytes);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == Regional.regionSpace) return ra;
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
    if (Region.verbose()) {
      Log.write("Mutator ");
      Log.writeln(Phase.getName(phaseId));
    }
    if (phaseId == Regional.PREPARE) {
      ra.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == Regional.RELEASE) {
      ra.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == Regional.EVACUATE_PREPARE) {
      ra.reset();
      super.collectionPhase(Regional.PREPARE, primary);
      return;
    }

    if (phaseId == Regional.EVACUATE_RELEASE) {
      ra.reset();
      super.collectionPhase(Regional.RELEASE, primary);
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
  Regional global() {
    return (Regional) VM.activePlan.global();
  }
}
