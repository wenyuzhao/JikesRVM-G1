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
package org.mmtk.plan.markcopy.remset;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.Space;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.alloc.MarkBlockAllocator;
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
 * See {@link MarkCopy} for an overview of the semi-space algorithm.
 *
 * @see MarkCopy
 * @see MarkCopyCollector
 * @see StopTheWorldMutator
 * @see MutatorContext
 */
@Uninterruptible
public class MarkCopyMutator extends StopTheWorldMutator {
  /****************************************************************************
   * Instance fields
   */
  protected final MarkBlockAllocator mc;
  private static final int REMSET_LOG_BUFFER_SIZE = 256;
  private AddressArray remSetLogBuffer = AddressArray.create(REMSET_LOG_BUFFER_SIZE);
  private int remSetLogBufferCursor = 0;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor
   */
  public MarkCopyMutator() {
    mc = new MarkBlockAllocator(MarkCopy.markBlockSpace, false);
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
    if (allocator == MarkCopy.ALLOC_MC) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes <= MarkBlock.BYTES_IN_BLOCK);
      return mc.alloc(bytes, align, offset);
    } else {
      return super.alloc(bytes, align, offset, allocator, site);
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference object, ObjectReference typeRef,
      int bytes, int allocator) {
    if (MarkBlock.Card.isEnabled()) MarkBlock.Card.updateCardMeta(object);
    if (allocator == MarkCopy.ALLOC_MC) {
      MarkCopy.markBlockSpace.postAlloc(object, bytes);
    } else {
      super.postAlloc(object, typeRef, bytes, allocator);
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == MarkCopy.markBlockSpace) return mc;
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
    if (phaseId == MarkCopy.PREPARE) {
      super.collectionPhase(phaseId, primary);
      mc.reset();
      return;
    }

    if (phaseId == MarkCopy.RELEASE) {
      mc.reset();
      super.collectionPhase(phaseId, primary);
      return;
    }

    if (phaseId == MarkCopy.RELOCATION_SET_SELECTION_PREPARE) {
      mc.reset();
      return;
    }

    if (phaseId == MarkCopy.RELOCATE_PREPARE) {
      super.collectionPhase(MarkCopy.PREPARE, primary);
      mc.reset();
      return;
    }
    if (phaseId == MarkCopy.RELOCATE_RELEASE) {
      super.collectionPhase(MarkCopy.RELEASE, primary);
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    mc.reset();
  }

  @Inline
  private void markAndEnqueueCard(Address card) {
    if (CardTable.cardIsMarked(card)) return;
    CardTable.markCard(card, true);
    if (remSetLogBufferCursor < remSetLogBuffer.length()) {
      remSetLogBuffer.set(remSetLogBufferCursor++, card);
    } else {
      ConcurrentRemSetRefinement.enqueueFilledRSBuffer(remSetLogBuffer);
      remSetLogBuffer = AddressArray.create(REMSET_LOG_BUFFER_SIZE);
      remSetLogBuffer.set(0, card);
      remSetLogBufferCursor = 1;
    }
  }

  @Inline
  private void checkCrossRegionPointer(ObjectReference src, Address slot, Address value) {
    if (!src.isNull() && !slot.isZero() && !value.isZero() && Space.isInSpace(MarkCopy.MC, src) && Space.isInSpace(MarkCopy.MC, slot) && Space.isInSpace(MarkCopy.MC, value)) {
      Word tmp = slot.toWord().xor(value.toWord());
      tmp = tmp.rshl(MarkBlock.LOG_BYTES_IN_BLOCK);
      tmp = value.isZero() ? Word.zero() : tmp;
      if (tmp.isZero()) return;
      markAndEnqueueCard(MarkBlock.Card.of(VM.objectModel.objectStartRef(src)));
    }
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
    checkCrossRegionPointer(src, slot, value.toAddress());
    VM.barriers.objectReferenceWrite(src, value, metaDataA, metaDataB, mode);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, value, metaDataA, metaDataB, mode);
    checkCrossRegionPointer(src, slot, value.toAddress());
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
    Address srcCursor = src.toAddress().plus(srcOffset);
    Address cursor = dst.toAddress().plus(dstOffset);
    Address limit = cursor.plus(bytes);
    while (cursor.LT(limit)) {
      ObjectReference ref = srcCursor.loadObjectReference();
      checkCrossRegionPointer(dst, cursor, ref.toAddress());
      cursor = cursor.plus(BYTES_IN_ADDRESS);
      srcCursor = srcCursor.plus(BYTES_IN_ADDRESS);
    }
    return false;
  }

  @Inline
  MarkCopy global() {
    return (MarkCopy) VM.activePlan.global();
  }
}
