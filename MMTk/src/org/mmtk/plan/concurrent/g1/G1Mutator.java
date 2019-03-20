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
package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.*;
import org.mmtk.plan.concurrent.ConcurrentMutator;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.HeaderByte;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.BYTES_IN_ADDRESS;

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
public class G1Mutator extends ConcurrentMutator {
  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator g1Eden = new RegionAllocator(G1.regionSpace, Region.EDEN);
  private Address remSetLogBuffer = Address.zero();
  private int remSetLogBufferCursor = 0;
  private final ObjectReferenceDeque modbuf;

  public G1Mutator() {
    modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
  }

  /****************************************************************************
   *
   * Initialization
   */

  @Inline
  private Address remSetLogBuffer() {
    if (remSetLogBuffer.isZero())
      remSetLogBuffer = G1.remsetLogBufferPool.alloc();
    return remSetLogBuffer;
  }

  @Override
  public void initMutator(int id) {
    super.initMutator(id);
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
    if (allocator == G1.ALLOC_EDEN) {
      return g1Eden.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    Region.Card.updateCardMeta(object);
    if (allocator == G1.ALLOC_EDEN) {
//      G1.regionSpace.initializeHeader(object, bytes);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == G1.regionSpace) return g1Eden;
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
//    Log.write("[Mutator] ");
//    Log.writeln(Phase.getName(phaseId));
    if (phaseId == G1.PREPARE) {
//      VM.collection.prepareMutator(this);
      super.collectionPhase(phaseId, primary);
      modbuf.resetLocal();
      g1Eden.reset();
      return;
    }

    if (phaseId == G1.RELEASE) {
      modbuf.flushLocal();
      g1Eden.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.RELOCATION_SET_SELECTION) {
      g1Eden.reset();
      return;
    }

    if (phaseId == G1.REFINE_CARDS) {
      dropCurrentRSBuffer();
      return;
    }

    if (phaseId == G1.FORWARD_PREPARE) {
//      VM.collection.prepareMutator(this);
      g1Eden.reset();
      super.collectionPhase(G1.PREPARE, primary);
      return;
    }

    if (phaseId == G1.FORWARD_RELEASE) {
      g1Eden.reset();
      super.collectionPhase(G1.RELEASE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    modbuf.flushLocal();
    g1Eden.reset();
    assertRemsetsFlushed();
  }

  @Override
  public void deinitMutator() {
    enqueueCurrentRSBuffer(true);
    super.deinitMutator();
  }

  @Inline
  public void dropCurrentRSBuffer() {
    if (remSetLogBufferCursor != 0) {
      remSetLogBufferCursor = 0;
    }
    if (!remSetLogBuffer.isZero()) {
      G1.remsetLogBufferPool.free(remSetLogBuffer);
//      Plan.metaDataSpace.release(remSetLogBuffer);
      remSetLogBuffer = Address.zero();
    }
  }

  @Inline
  public void enqueueCurrentRSBuffer(boolean triggerConcurrentRefinement) {
    if (remSetLogBufferCursor == 0) return;

    Address buf = remSetLogBuffer;
    remSetLogBuffer = Address.zero();
    remSetLogBufferCursor = 0;
    ConcurrentRemSetRefinement.enqueueFilledRSBuffer(buf, triggerConcurrentRefinement);
  }

  @NoInline
  public void rsEnqueue(ObjectReference src) {
    Address card = Region.Card.of(src);
    if (CardTable.attemptToMarkCard(card, true)) {
      remSetLogBuffer().plus(remSetLogBufferCursor << Constants.LOG_BYTES_IN_ADDRESS).store(card);
      remSetLogBufferCursor += 1;
      if (remSetLogBufferCursor >= G1.REMSET_LOG_BUFFER_SIZE) {
        enqueueCurrentRSBuffer(true);
      }
    }
  }

  @Inline
  public void checkCrossRegionPointer(ObjectReference src, Address slot, ObjectReference ref) {
    Word x = VM.objectModel.refToAddress(src).toWord();
    Word y = VM.objectModel.refToAddress(ref).toWord();
    Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
    tmp = ref.isNull() ? Word.zero() : tmp;
    if (!tmp.isZero()) {
//      Log.writeln("root ", ref);
      rsEnqueue(src);
    }
  }

  @Override
  @Inline
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (!ref.isNull()) {
      modbuf.insert(ref);
    }
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
    super.objectReferenceWrite(src, slot, value, metaDataA, metaDataB, mode);
    checkCrossRegionPointer(src, slot, value);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
    boolean result = super.objectReferenceTryCompareAndSwap(src, slot, old, value, metaDataA, metaDataB, mode);
    checkCrossRegionPointer(src, slot, value);
    return result;
  }

  @Override
  public final void assertRemsetsFlushed() {
//    VM.activePlan.getNextMutator()
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(modbuf.isFlushed());
//    }
  }

  @Inline
  G1 global() {
    return (G1) VM.activePlan.global();
  }
}
