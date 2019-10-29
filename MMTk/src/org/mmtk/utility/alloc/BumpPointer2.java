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
package org.mmtk.utility.alloc;

import org.mmtk.plan.Plan;
import org.mmtk.policy.ImmortalSpace;
import org.mmtk.policy.Space;
import org.mmtk.policy.region.Card;
import org.mmtk.utility.Conversions;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.*;

@Uninterruptible public class BumpPointer2 extends Allocator {

  protected static final int LOG_BLOCK_SIZE = LOG_BYTES_IN_PAGE + 3;
  protected static final Word BLOCK_MASK = Word.one().lsh(LOG_BLOCK_SIZE).minus(Word.one());
  private static final int BLOCK_SIZE = (1 << LOG_BLOCK_SIZE);
  private static final int SIZE_OF_TWO_X86_CACHE_LINES_IN_BYTES = 128;

  protected Address cursor = Address.zero();
  private Address limit = Address.zero();
  protected Space space;

  public BumpPointer2(Space space) {
    this.space = space;
    reset();
  }

  public final void rebind(Space space) {
    reset();
    this.space = space;
  }


  @Inline
  private void setLimit(Address cursor, Address limit) {
    if (!this.cursor.isZero() && !this.limit.isZero()) {
      fillAlignmentGap(this.cursor, this.limit);
    }
    this.cursor = cursor;
    this.limit = limit;
  }

  public final void reset() {
    if (!this.cursor.isZero() && !this.limit.isZero()) {
      fillAlignmentGap(this.cursor, this.limit);
    }
    cursor = Address.zero();
    limit = Address.zero();
  }

  @Inline
  public final Address alloc(int bytes, int align, int offset) {
    Address start = alignAllocationNoFill(cursor, align, offset);
    Address end = start.plus(bytes);
    if (end.GT(limit))
      return allocSlow(bytes, align, offset);
    fillAlignmentGap(cursor, start);
    cursor = end;
    end.plus(SIZE_OF_TWO_X86_CACHE_LINES_IN_BYTES).prefetch();
    return start;
  }

  @Override
  protected final Address allocSlowOnce(int size, int align, int offset) {
    Extent blockSize = Word.fromIntZeroExtend(size).plus(BLOCK_MASK).and(BLOCK_MASK.not()).toExtent();
    Address acquiredStart = space.acquire(Conversions.bytesToPages(blockSize));
    if (acquiredStart.isZero()) {
      return acquiredStart;
    } else {
//      if (VM.VERIFY_ASSERTIONS) {
//        VM.assertions._assert(acquiredStart.NE(EmbeddedMetaData.getMetaDataBase(acquiredStart)));
//      }
      if (acquiredStart.EQ(EmbeddedMetaData.getMetaDataBase(acquiredStart))) {
        acquiredStart = acquiredStart.plus(ImmortalSpace.META_DATA_PAGES_PER_REGION << LOG_BYTES_IN_PAGE);
      }
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(acquiredStart));
      Address card = Card.of(acquiredStart);
      createCardAnchor(card, acquiredStart, blockSize.toInt());
      setLimit(acquiredStart, acquiredStart.plus(blockSize));
      return alloc(size, align, offset);
    }
  }

  @Inline
  private void createCardAnchor(Address card, Address start, int bytes) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!start.isZero());
    while (bytes > 0) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(card));
      getCardMetaData(card).store(start);
      card = card.plus(Card.BYTES_IN_CARD);
      bytes -= Card.BYTES_IN_CARD;
    }
  }

  @Inline
  private static Address getCardMetaData(Address card) {
    Address metadata = EmbeddedMetaData.getMetaDataBase(card);
    Extent offset = EmbeddedMetaData.getMetaDataOffset(card, Card.LOG_BYTES_IN_CARD - LOG_CARD_META_SIZE, LOG_CARD_META_SIZE);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(offset.toWord().EQ(offset.toWord().and((Word.fromIntZeroExtend(BYTES_IN_WORD).minus(Word.one()).not()))));
    return metadata.plus(offset);
  }

  @Inline
  public static final void linearScan(Address card, Card.LinearScan scanner, boolean markDead, Object context) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(card));
    Address cursor = getCardMetaData(card).loadAddress();
    if (cursor.isZero()) return;
    final Address limit = card.plus(1 << Card.LOG_BYTES_IN_CARD);
    boolean shouldUpdateAnchor = cursor.LT(card);

    while (cursor.LT(limit)) {
      ObjectReference object = Card.getObjectFromStartAddress(cursor, limit);
      if (object.isNull() || tibIsZero(object)) return;
      Address startRef = VM.objectModel.objectStartRef(object);

      if (shouldUpdateAnchor && startRef.GE(card)) {
        shouldUpdateAnchor = false;
        getCardMetaData(card).store(startRef);
      }
      if (startRef.GE(limit)) return;
      cursor = VM.objectModel.getObjectEndAddress(object);
      if (startRef.GE(card) && startRef.LT(limit)) {
        if (markDead) {
          if (Plan.immortalSpace.isMarked(object) && !isDead(object)) {
            scanner.scan(card, object, context);
          } else {
            markAsDead(object);
          }
        } else {
          if (!isDead(object)) scanner.scan(card, object, context);
        }
      }
    }
  }


  /** @return the space associated with this bump pointer */
  @Override
  public final Space getSpace() {
    return space;
  }

  @Inline
  public static boolean tibIsZero(ObjectReference o) {
    return VM.objectModel.refToAddress(o).loadAddress().isZero();
  }

  private static final byte DEATH_BIT = 1 << 3;

  @Inline
  private static void markAsDead(ObjectReference object) {
    byte value = VM.objectModel.readAvailableByte(object);
    VM.objectModel.writeAvailableByte(object, (byte) (value | DEATH_BIT));
  }

  @Inline
  private static boolean isDead(ObjectReference object) {
    byte value = VM.objectModel.readAvailableByte(object);
    return ((byte) (value & DEATH_BIT)) == DEATH_BIT;
  }
}
