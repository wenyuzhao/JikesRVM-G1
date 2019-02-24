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
package org.mmtk.plan.regional.remsetbarrier;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.*;

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
  protected final RegionAllocator ra;
//  private final TraceWriteBuffer markRemset = new TraceWriteBuffer(global().markTrace);

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public G1Mutator() {
    super();
    ra = new RegionAllocator(G1.regionSpace, Region.NORMAL);
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(initialBuffer != null);
//    Address a = VM.objectModel.objectAsAddress(initialBuffer);// ObjectReference.fromObject(initialBuffer);
//    remSetLogBuffer = a;
//    barrierActive = false;
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
    if (allocator == G1.ALLOC_MC) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_REGION);
      return ra.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    if (allocator == G1.ALLOC_MC) {
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
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    //Log.write("[Mutator] ");
    //Log.writeln(Phase.getName(phaseId));
    if (phaseId == G1.PREPARE) {
//      barrierActive = false;
      ra.reset();
//      markRemset.flush();
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
//      barrierActive = false;
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void initMutator(int id) {
    super.initMutator(id);
//    G1.offHeapMetaDataSpace.acquire(1);
//    if (G1.isInitialized()) {
//      remSetLogBuffer = G1.offHeapMetaDataSpace.acquire(1);
//    } else {
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!remSetLogBuffer.isZero());
//    }
  }

  @Override
  public void deinitMutator() {
    enqueueCurrentRSBuffer(true, false);
    super.deinitMutator();
  }

  private static final int REMSET_LOG_BUFFER_SIZE = Constants.BYTES_IN_PAGE >>> Constants.LOG_BYTES_IN_ADDRESS;
//  int[] initialBuffer = new int[REMSET_LOG_BUFFER_SIZE];
  Address remSetLogBuffer = Address.zero();// VM.objectModel.objectAsAddress(initialBuffer);// = ObjectReference.fromObject(initialBuffer).toAddress();//Address.zero();//G1.offHeapMetaDataSpace.acquire(1);
  private int remSetLogBufferCursor = 0;

  @Inline
  private Address remSetLogBuffer() {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!remSetLogBuffer.isZero());
    if (remSetLogBuffer.isZero())
      remSetLogBuffer = Plan.offHeapMetaDataSpace.acquire(1);
    return remSetLogBuffer;
  }

  @Inline
  public void dropCurrentRSBuffer() {
    if (remSetLogBufferCursor != 0) {
//      remSetLogBufferCursor = 0;
//      remSetLogBuffer = AddressArray.create(256);
    }
//    if (!remSetLogBuffer.isZero()) {
//      Plan.metaDataSpace.release(remSetLogBuffer);
//      remSetLogBuffer = Address.zero();
//    }
  }

  static AddressArray globalRemSetLogBuffer = AddressArray.create(1000);
  static int globalRemSetLogBufferCursor = 0;
//  static int maxGlobalRemSetLogBufferCursor = BYTES_IN_PAGE >> LOG_BYTES_IN_ADDRESS;

//  @NoInline
  public void enqueueCurrentRSBuffer(boolean triggerConcurrentRefinement, boolean acquireNewBuffer) {
    if (remSetLogBufferCursor == 0) return;
    // Add to globalRemSetLogBuffer
    if (globalRemSetLogBufferCursor >= globalRemSetLogBuffer.length()) {
      VM.assertions.fail("GlobalRemSetBuffer Overflow");
    }
    globalRemSetLogBuffer.set(globalRemSetLogBufferCursor, remSetLogBuffer);
    globalRemSetLogBufferCursor += 1;
    // Acquire new buffer
    remSetLogBuffer = G1.offHeapMetaDataSpace.acquire(1);
    remSetLogBufferCursor = 0;
  }

  @NoInline
  public void markAndEnqueueCard(Address card) {
    if (CardTable.attemptToMarkCard(card, true)) {
      remSetLogBuffer().plus(remSetLogBufferCursor << LOG_BYTES_IN_ADDRESS).store(card);
      remSetLogBufferCursor += 1;
      if (remSetLogBufferCursor >= REMSET_LOG_BUFFER_SIZE) {
        enqueueCurrentRSBuffer(true, true);
      }
    }
  }

  @Inline
  public void checkCrossRegionPointer(ObjectReference src, Address slot, ObjectReference ref) {
//    if (src.toAddress().GE(VM.AVAILABLE_START) && !ref.isNull()) {
//      VM.assertions._assert(false);
      Word x = VM.objectModel.refToAddress(src).toWord();
      Word y = VM.objectModel.refToAddress(ref).toWord();
      Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
      tmp = ref.isNull() ? Word.zero() : tmp;
      if (!tmp.isZero() && Space.isInSpace(G1.RS, ref)) {
        markAndEnqueueCard(Region.Card.of(src));
      }
//    }
////    if (!ref.isNull() && !src.isNull()) {
//      Word x = VM.objectModel.refToAddress(src).toWord();
//      Word y = VM.objectModel.refToAddress(ref).toWord();
//      Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
//      if (!tmp.isZero() && !ref.isNull() && Space.isInSpace(G1.RS, ref) && x.toAddress().GE(VM.AVAILABLE_START)) {
////        Region.Card.assertCardMeta(src);
//        markAndEnqueueCard(Region.Card.of(src));
//      }
////    }
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.objectReferenceWrite(src, value, metaDataA, metaDataB, mode);
    checkCrossRegionPointer(src, slot, value);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, value, metaDataA, metaDataB, mode);
    checkCrossRegionPointer(src, slot, value);
    return result;
  }

  @Inline
  @Override
  public boolean objectReferenceBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    VM.assertions.fail("unreachable???");
//    Address cursor = dst.toAddress().plus(dstOffset);
//    Address limit = cursor.plus(bytes);
//    while (cursor.LT(limit)) {
//      ObjectReference ref = cursor.loadObjectReference();
//      if (barrierActive) checkAndEnqueueReference(ref);
//      cursor = cursor.plus(BYTES_IN_ADDRESS);
//    }
//    return false;
//
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
  public void flushRememberedSets() {
    ra.reset();
    assertRemsetsFlushed();
  }

  @Inline
  G1 global() {
    return (G1) VM.activePlan.global();
  }
}
