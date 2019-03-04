package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;

@Uninterruptible
public class CardTable {
  static final int TOTAL_CARDS;
  static final int HOTNESS_TABLE_PAGES;
  static final byte HOTNESS_THRESHOLD = 4;
  public static final int[] cardTable;
  static final int[] dirtyCards = new int[] { 0 };
  static Address cardHotnessTable = Address.zero();
  static final Lock cardHotnessTableLock = VM.newLock("cardHotnessTableLock");

  static {
    int memorySize = VM.HEAP_END.diff(VM.HEAP_START).toInt();
    TOTAL_CARDS = memorySize >>> Region.Card.LOG_BYTES_IN_CARD;
    cardTable = new int[TOTAL_CARDS >>> Constants.LOG_BITS_IN_INT];
    HOTNESS_TABLE_PAGES = RemSet.ceilDiv(TOTAL_CARDS, Constants.BYTES_IN_PAGE);
  }

  @Inline
  public static boolean increaseHotness(Address card) {
    if (cardHotnessTable.isZero()) {
      cardHotnessTableLock.acquire();
      if (cardHotnessTable.isZero()) {
        cardHotnessTable = Plan.metaDataSpace.acquire(HOTNESS_TABLE_PAGES);
      }
      cardHotnessTableLock.release();
    }
    final int index = hash(card);
    final int intIndex = index >>> 2;
    final int byteIndex = index ^ (intIndex << 2);
    final Address hotnessPtr = cardHotnessTable.plus(intIndex << Constants.LOG_BYTES_IN_INT);
    int oldValue, newValue;
    byte newHotness;
    do {
      // Get old int
      oldValue = hotnessPtr.prepareInt();
      byte oldHotness = (byte) ((oldValue << (byteIndex << 3)) >>> 24);
      if (oldHotness >= HOTNESS_THRESHOLD) return true; // This is a hot card
      // Build new int
      newHotness = (byte) (oldHotness + 1);
      newValue = oldValue & ~(0xff << ((3 - byteIndex) << 3)); // Drop the target byte
      newValue |= (newHotness << ((3 - byteIndex) << 3)); // Set new byte
    } while (!hotnessPtr.attempt(oldValue, newValue));
    return newHotness > HOTNESS_THRESHOLD;
  }

  @Inline
  public static void clearAllHotness() {
    if (cardHotnessTable.isZero()) return;
    VM.memory.zero(false, cardHotnessTable, Extent.fromIntZeroExtend(HOTNESS_TABLE_PAGES << Constants.LOG_BYTES_IN_PAGE));
  }

  @Inline
  public static int dirtyCardSize() {
    return dirtyCards[0];
  }

  @Inline
  private static int hash(Address card) {
    return card.diff(VM.HEAP_START).toWord().rshl(Region.Card.LOG_BYTES_IN_CARD).toInt();
  }

  @Inline
  public static boolean attemptToMarkCard(Address card, boolean mark) {
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Region.Card.of(card)));
    int cardIndex = hash(card);
    boolean success = attemptBitInBuffer(cardTable, cardIndex, mark);
    if (success) {
      Address dirtyCardsPointer = ObjectReference.fromObject(dirtyCards).toAddress();
      int oldValue, newValue;
      int delta = mark ? 1 : -1;
      do {
        oldValue = dirtyCardsPointer.prepareInt();
        newValue = oldValue + delta;
      } while (!dirtyCardsPointer.attempt(oldValue, newValue));
    }
    return success;
  }

  @Inline
  private static boolean attemptBitInBuffer(int[] buf, int index, boolean newBit) {
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldBit != newBit);
    int intIndex = index >>> Constants.LOG_BITS_IN_INT;
    int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
    Address addr = ObjectReference.fromObject(buf).toAddress().plus(intIndex << Constants.LOG_BYTES_IN_INT);
//    Offset offset = Offset.fromIntZeroExtend(intIndex << Constants.LOG_BYTES_IN_INT);
    int oldValue, newValue;
    do {
      // Get old int
      oldValue = addr.prepareInt();//buf[intIndex];
      boolean oldBit = (oldValue & (1 << (31 - bitIndex))) != 0;
      if (oldBit == newBit) return false;
      // Build new int
      if (newBit) {
        newValue = oldValue | (1 << (31 - bitIndex));
      } else {
        newValue = oldValue & (~(1 << (31 - bitIndex)));
      }
      if (oldValue == newValue) return false; // this bit has been set by other threads
    } while (!addr.attempt(oldValue, newValue));
    return true;
  }

  @Inline
  private static boolean getBit(int[] buf, int index) {
    int intIndex = index >>> Constants.LOG_BITS_IN_INT;
    int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(intIndex == index / 32 && bitIndex == index % 32);
    Address addr = ObjectReference.fromObject(buf).toAddress().plus(intIndex << Constants.LOG_BYTES_IN_INT);
//    int entry = buf[intIndex];
    int entry = addr.loadInt();
    return (entry & (1 << (31 - bitIndex))) != 0;
    //return ((entry << bitIndex) >> (Constants.LOG_BITS_IN_INT - 1)) > 0;
  }

  @Inline
  public static boolean cardIsMarked(Address card) {
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Region.Card.of(card)));
    int cardIndex = hash(card);
    return getBit(cardTable, cardIndex);
  }

  public static void clearAllCardMarks() {
    for (int i = 0; i < cardTable.length; i++) {
      cardTable[i] = 0;
    }
  }
}
