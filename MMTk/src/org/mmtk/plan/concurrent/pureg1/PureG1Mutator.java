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
import org.mmtk.plan.concurrent.ConcurrentMutator;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.RegionAllocator;
import org.mmtk.utility.deque.AddressDeque;
import org.mmtk.vm.Lock;
import org.mmtk.vm.Monitor;
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
 * See {@link PureG1} for an overview of the semi-space algorithm.
 *
 * @see PureG1
 * @see PureG1Collector
 * @see StopTheWorldMutator
 * @see MutatorContext
 */
@Uninterruptible
public class PureG1Mutator extends ConcurrentMutator {
  /****************************************************************************
   * Instance fields
   */
  protected final RegionAllocator mc;
  private static final int REMSET_LOG_BUFFER_SIZE = ConcurrentRemSetRefinement.REMSET_LOG_BUFFER_SIZE;
  private Address remSetLogBuffer = Address.zero();// = AddressArray.create(REMSET_LOG_BUFFER_SIZE);
  private int remSetLogBufferCursor = 0;
  private final TraceWriteBuffer markRemset, relocateRemset;
  private TraceWriteBuffer currentRemset;
  //private final AddressDeque cardBuf;
  // public final Lock refinementLock = VM.newLock("refinementLock");
  // private boolean refinementInProgress = false;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public PureG1Mutator() {
    mc = new RegionAllocator(PureG1.regionSpace, false);
    markRemset = new TraceWriteBuffer(global().markTrace);
    relocateRemset = new TraceWriteBuffer(global().redirectTrace);
    currentRemset = markRemset;
    //cardBuf = new AddressDeque("cardBuf", ConcurrentRemSetRefinement.cardBufPool);
  }

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
    if (allocator == PureG1.ALLOC_MC) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= Region.BYTES_IN_BLOCK);
      return mc.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef, int bytes, int allocator) {
    Region.Card.updateCardMeta(object);
    if (allocator == PureG1.ALLOC_MC) {
      /*if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(Space.isInSpace(PureG1.MC, object));
        VM.assertions._assert(Region.allocated(Region.of(VM.objectModel.objectStartRef(object))));
        VM.assertions._assert(Region.of(object).NE(EmbeddedMetaData.getMetaDataBase(VM.objectModel.objectStartRef(object))));
      }*/
      PureG1.regionSpace.postAlloc(object, bytes);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == PureG1.regionSpace) return mc;
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
    if (phaseId == PureG1.PREPARE) {
//      enqueueCurrentRSBuffer(true);
      currentRemset = markRemset;
      super.collectionPhase(phaseId, primary);
      mc.reset();
      return;
    }

    if (phaseId == PureG1.RELEASE) {
      mc.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == PureG1.REMEMBERED_SETS) {
      //Log.writeln("Mutator #", getId());
      return;
    }

    if (phaseId == PureG1.RELOCATION_SET_SELECTION_PREPARE) {
      mc.reset();
      return;
    }

    if (phaseId == PureG1.REDIRECT_PREPARE) {
      VM.collection.prepareMutator(this);
      currentRemset = relocateRemset;
      mc.reset();
//      enqueueCurrentRSBuffer(false);
      super.collectionPhase(PureG1.PREPARE, primary);
      //if (barrierActive) {
      //  Log.writeln("BarrierActive for mutator #", getId());
      //}
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!barrierActive);
      return;
    }

    if (phaseId == PureG1.CONSTRUCT_REMEMBERED_SETS) {
      VM.collection.prepareMutator(this);
//      currentRemset = relocateRemset;
      mc.reset();
      return;
    }

    if (phaseId == PureG1.REDIRECT_RELEASE) {
      mc.reset();
      super.collectionPhase(PureG1.RELEASE, primary);
      return;
    }

    if (phaseId == PureG1.COMPLETE) {
      mc.reset();
//      super.collectionPhase(PureG1.COMPLETE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    //enqueueCurrentRSBuffer();
    currentRemset.flush();
    mc.reset();
//    enqueueCurrentRSBuffer(false);
    //cardBuf.flushLocal();
    assertRemsetsFlushed();
  }

  @Override
  public void deinitMutator() {
    enqueueCurrentRSBuffer(true);
    super.deinitMutator();
  }

  public boolean rsBufferIsEmpty() {
    return remSetLogBufferCursor == 0;
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

  //static Monitor lock = VM.newLock("awewwenyu");

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
  private void checkCrossRegionPointer(ObjectReference src, Address slot, ObjectReference ref) {
    if (!ref.isNull() && !src.isNull()) {
      Word x = VM.objectModel.objectStartRef(src).toWord();
      Word y = VM.objectModel.objectStartRef(ref).toWord();
      Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_BLOCK);
      if (!tmp.isZero() && Space.isInSpace(PureG1.MC, ref)) {
        Region.Card.updateCardMeta(src);
        markAndEnqueueCard(Region.Card.of(src));
      }
    }
  }

  @Override
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (ref.isNull()) return;
    //if (barrierActive) {

    if (Space.isInSpace(PureG1.MC, ref)) PureG1.regionSpace.traceMarkObject(currentRemset, ref);
    else if (Space.isInSpace(PureG1.IMMORTAL, ref)) PureG1.immortalSpace.traceObject(currentRemset, ref);
    else if (Space.isInSpace(PureG1.LOS, ref)) PureG1.loSpace.traceObject(currentRemset, ref);
    else if (Space.isInSpace(PureG1.NON_MOVING, ref)) PureG1.nonMovingSpace.traceObject(currentRemset, ref);
    else if (Space.isInSpace(PureG1.SMALL_CODE, ref)) PureG1.smallCodeSpace.traceObject(currentRemset, ref);
    else if (Space.isInSpace(PureG1.LARGE_CODE, ref)) PureG1.largeCodeSpace.traceObject(currentRemset, ref);
    //}

    /*if (VM.VERIFY_ASSERTIONS) {
      if (!ref.isNull() && !Plan.gcInProgress()) {
        if (Space.isInSpace(PureG1.MC, ref)) VM.assertions._assert(PureG1.regionSpace.isLive(ref));
        else if (Space.isInSpace(PureG1.IMMORTAL, ref)) VM.assertions._assert(PureG1.immortalSpace.isLive(ref));
        else if (Space.isInSpace(PureG1.LOS, ref)) VM.assertions._assert(PureG1.loSpace.isLive(ref));
        else if (Space.isInSpace(PureG1.NON_MOVING, ref)) VM.assertions._assert(PureG1.nonMovingSpace.isLive(ref));
        else if (Space.isInSpace(PureG1.SMALL_CODE, ref)) VM.assertions._assert(PureG1.smallCodeSpace.isLive(ref));
        else if (Space.isInSpace(PureG1.LARGE_CODE, ref)) VM.assertions._assert(PureG1.largeCodeSpace.isLive(ref));
      }
    }*/
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
    boolean containsCrossRegionPointer = false;
    while (cursor.LT(limit)) {
//      ObjectReference oldValue, newValue;
//      do {
//        oldValue = cursor.loadObjectReference();
//        newValue = srcCursor.loadObjectReference();
//      } while (!cursor.attempt(oldValue, newValue));

      ObjectReference element = srcCursor.loadObjectReference();
      cursor.store(element);
      if (!containsCrossRegionPointer) {
        if (!element.isNull()) {
          Word x = VM.objectModel.objectStartRef(dst).toWord();
          Word y = VM.objectModel.objectStartRef(element).toWord();
          Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_BLOCK);
          if (!tmp.isZero() && Space.isInSpace(PureG1.MC, element)) {
            containsCrossRegionPointer = true;
          }
        }
      }
      cursor = cursor.plus(BYTES_IN_ADDRESS);
      srcCursor = srcCursor.plus(BYTES_IN_ADDRESS);
    }
    if (containsCrossRegionPointer) {
      markAndEnqueueCard(Region.Card.of(dst));
    }
    return true;
  }

  @Override
  public final void assertRemsetsFlushed() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(currentRemset.isFlushed());
      //VM.assertions._assert(remSetLogBufferCursor == 0);
    }
  }

  @Inline
  PureG1 global() {
    return (PureG1) VM.activePlan.global();
  }
}
