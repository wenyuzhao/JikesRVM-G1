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
package org.mmtk.plan.regional.rust;

//import org.mmtk.plan.*;
import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Phase;
import org.mmtk.plan.Plan;
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
 * @see MutatorContext
 */
@Uninterruptible
public class RegionalMutator extends MutatorContext {

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
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return ra.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    if (allocator == Regional.ALLOC_MC) {
      Regional.regionSpace.initializeHeader(object, bytes);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
  }

  @Override
  @Inline
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
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    //Log.write("[Mutator] ");
    //Log.writeln(Phase.getName(phaseId));
    if (phaseId == Regional.PREPARE_STACKS) {
      if (!Plan.stacksPrepared()) {
        VM.collection.prepareMutator(this);
      }
      flushRememberedSets();
      return;
    }

    if (phaseId == Regional.PREPARE) {
      ra.reset();
      los.prepare(true);
      lgcode.prepare(true);
      smcode.prepare();
      nonmove.prepare();
      VM.memory.collectorPrepareVMSpace();
      return;
    }

    if (phaseId == Regional.RELEASE) {
      ra.reset();
      los.release(true);
      lgcode.release(true);
      smcode.release();
      nonmove.release();
      VM.memory.collectorReleaseVMSpace();
      return;
    }

    if (phaseId == Regional.EVACUATE_PREPARE) {
      ra.reset();
      los.prepare(true);
      lgcode.prepare(true);
      smcode.prepare();
      nonmove.prepare();
      VM.memory.collectorPrepareVMSpace();
      return;
    }

    if (phaseId == Regional.EVACUATE_RELEASE) {
      ra.reset();
      los.release(true);
      lgcode.release(true);
      smcode.release();
      nonmove.release();
      VM.memory.collectorReleaseVMSpace();
      return;
    }

//    super.collectionPhase(phaseId, primary);


    Log.write("Per-mutator phase \"");
    Log.write(Phase.getName(phaseId));
    Log.writeln("\" not handled.");
    VM.assertions.fail("Per-mutator phase not handled!");
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
