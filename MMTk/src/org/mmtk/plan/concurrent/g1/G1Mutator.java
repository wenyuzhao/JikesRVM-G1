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
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
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
  protected final RegionAllocator g1Survivor = new RegionAllocator(G1.regionSpace, Region.SURVIVOR);
  protected final RegionAllocator g1Old = new RegionAllocator(G1.regionSpace, Region.OLD);
  private static final int REMSET_LOG_BUFFER_SIZE = ConcurrentRemSetRefinement.REMSET_LOG_BUFFER_SIZE;
  private Address remSetLogBuffer = Address.zero();
  private int remSetLogBufferCursor = 0;
  private final TraceWriteBuffer markRemset = new TraceWriteBuffer(global().markTrace);
  private TraceWriteBuffer currentRemset = markRemset;

  /****************************************************************************
   *
   * Initialization
   */

  @Inline
  private Address remSetLogBuffer() {
    if (remSetLogBuffer.isZero())
      remSetLogBuffer = Plan.metaDataSpace.acquire(1);
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
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return g1Eden.alloc(bytes, align, offset);
    } else if (allocator == G1.ALLOC_SURVIVOR) {
      VM.assertions.fail("");
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return g1Survivor.alloc(bytes, align, offset);
    } else if (allocator == G1.ALLOC_OLD) {
      VM.assertions.fail("");
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return g1Old.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    Region.Card.updateCardMeta(object);
    if (allocator == G1.ALLOC_EDEN || allocator == G1.ALLOC_SURVIVOR || allocator == G1.ALLOC_OLD) {
      G1.regionSpace.initializeHeader(object);
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
    //Log.write("[Mutator] ");
    //Log.writeln(Phase.getName(phaseId));
    if (phaseId == G1.PREPARE) {
      currentRemset = markRemset;
      super.collectionPhase(phaseId, primary);
      g1Eden.reset();
      g1Survivor.reset();
      g1Old.reset();
      return;
    }

    if (phaseId == G1.RELEASE) {
      g1Eden.reset();
      g1Survivor.reset();
      g1Old.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == G1.REFINE_CARDS) {
      dropCurrentRSBuffer();
      return;
    }

    if (phaseId == G1.FORWARD_PREPARE) {
      g1Eden.reset();
      g1Survivor.reset();
      g1Old.reset();
      super.collectionPhase(G1.PREPARE, primary);
      return;
    }

    if (phaseId == G1.FORWARD_RELEASE) {
      g1Eden.reset();
      g1Survivor.reset();
      g1Old.reset();
      super.collectionPhase(G1.RELEASE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    currentRemset.flush();
    g1Eden.reset();
    g1Survivor.reset();
    g1Old.reset();
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
      Plan.metaDataSpace.release(remSetLogBuffer);
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

  @Inline
  public void markAndEnqueueCard(Address card) {
    if (CardTable.attemptToMarkCard(card, true)) {
      remSetLogBuffer().plus(remSetLogBufferCursor << Constants.LOG_BYTES_IN_ADDRESS).store(card);
      remSetLogBufferCursor += 1;
      if (remSetLogBufferCursor >= REMSET_LOG_BUFFER_SIZE) {
        enqueueCurrentRSBuffer(true);
      }
    }
  }

  @Inline
  public void checkCrossRegionPointer(ObjectReference src, Address slot, ObjectReference ref) {
    if (!ref.isNull() && !src.isNull()) {
      Word x = VM.objectModel.objectStartRef(src).toWord();
      Word y = VM.objectModel.objectStartRef(ref).toWord();
      Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
      if (!tmp.isZero() && Space.isInSpace(G1.G1, ref)) {
        Region.Card.updateCardMeta(src);
        markAndEnqueueCard(Region.Card.of(src));
      }
    }
  }

  @Override
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (ref.isNull()) return;

    if (Space.isInSpace(G1.G1, ref)) G1.regionSpace.traceMarkObject(currentRemset, ref);
    else if (Space.isInSpace(G1.IMMORTAL, ref)) G1.immortalSpace.traceObject(currentRemset, ref);
    else if (Space.isInSpace(G1.LOS, ref)) G1.loSpace.traceObject(currentRemset, ref);
    else if (Space.isInSpace(G1.NON_MOVING, ref)) G1.nonMovingSpace.traceObject(currentRemset, ref);
    else if (Space.isInSpace(G1.SMALL_CODE, ref)) G1.smallCodeSpace.traceObject(currentRemset, ref);
    else if (Space.isInSpace(G1.LARGE_CODE, ref)) G1.largeCodeSpace.traceObject(currentRemset, ref);
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

  /**
   * {@inheritDoc}
   *
   * @param src The source of the values to be copied
   * @param srcOffset The offset of the first source address, in
   * bytes, relative to <code>src</code> (in principle, this could be
   * negative).
   * @param dst The mutated object, i.e. the destination of the copy.
   * @param dstOffset The offset of the first destination address, in
   * bytes relative to <code>tgt</code> (in principle, this could be
   * negative).
   * @param bytes The size of the region being copied, in bytes.
   */
  @Inline
  @Override
  public boolean objectReferenceBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    super.objectReferenceBulkCopy(src, srcOffset, dst, dstOffset, bytes);
    Address srcCursor = src.toAddress().plus(srcOffset);
    Address cursor = dst.toAddress().plus(dstOffset);
    Address limit = cursor.plus(bytes);
//    boolean containsCrossRegionPointer = false;
    while (cursor.LT(limit)) {
      ObjectReference element = srcCursor.loadObjectReference();
      cursor.store(element);
      checkCrossRegionPointer(dst, cursor, element);
//      if (!containsCrossRegionPointer) {
//        if (!element.isNull()) {
//          Word x = VM.objectModel.objectStartRef(dst).toWord();
//          Word y = VM.objectModel.objectStartRef(element).toWord();
//          Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
//          if (!tmp.isZero() && Space.isInSpace(G1.G1, element)) {
//            containsCrossRegionPointer = true;
//          }
//        }
//      }
      cursor = cursor.plus(BYTES_IN_ADDRESS);
      srcCursor = srcCursor.plus(BYTES_IN_ADDRESS);
    }
//    if (containsCrossRegionPointer) {
//      markAndEnqueueCard(Region.Card.of(dst));
//    }
    return true;
  }

  @Override
  public final void assertRemsetsFlushed() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(currentRemset.isFlushed());
    }
  }

  @Inline
  G1 global() {
    return (G1) VM.activePlan.global();
  }
}
