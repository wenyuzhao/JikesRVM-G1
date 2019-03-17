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
package org.mmtk.plan.concurrent.regional;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.plan.TraceWriteBuffer;
import org.mmtk.plan.concurrent.ConcurrentMutator;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.HeaderByte;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.deque.ObjectReferenceDeque;
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
public class RegionalMutator extends ConcurrentMutator {

  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator ra;
//  private final TraceWriteBuffer remset;
  private final ObjectReferenceDeque modbuf;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public RegionalMutator() {
    modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
    ra = new RegionAllocator(Regional.regionSpace, Region.NORMAL);
//    remset = new TraceWriteBuffer(global().markTrace);
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
    if (allocator == Regional.ALLOC_RS) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return ra.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    if (allocator == Regional.ALLOC_RS) {
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
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    //Log.write("[Mutator] ");
    //Log.writeln(Phase.getName(phaseId));
    if (phaseId == Regional.PREPARE) {
//      modbuf.
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(modbuf.isEmpty());
//      VM.collection.prepareMutator(this);
      super.collectionPhase(phaseId, primary);
      ra.reset();
      modbuf.resetLocal();
      return;
    }

    if (phaseId == Regional.RELEASE) {
      ra.reset();
      modbuf.flushLocal();
      super.collectionPhase(phaseId, primary);

      return;
    }

    if (phaseId == Regional.FORWARD_PREPARE) {
      ra.reset();
      super.collectionPhase(Regional.PREPARE, primary);
      return;
    }

    if (phaseId == Regional.FORWARD_RELEASE) {
      ra.reset();
      super.collectionPhase(Regional.RELEASE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
//    remset.flush();
    ra.reset();
    modbuf.flushLocal();
    assertRemsetsFlushed();
  }

  @Override
  @Inline
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (!ref.isNull()) {
//    if (HeaderByte.attemptLog(ref)) {
//      slow.add(1);
      modbuf.insert(ref);
    }
//    }
//    if (barrierActive) {
//      if (!ref.isNull()) {
//        if (HeaderByte.isUnlogged(ref)) {
//          HeaderByte.markAsLogged(ref);
//          modbuf.insert(ref);
//        }
//      }
//    }
  }

  @Override
  public final void assertRemsetsFlushed() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(modbuf.isFlushed());
    }
  }

  @Inline
  private final Regional global() {
    return (Regional) VM.activePlan.global();
  }
}
