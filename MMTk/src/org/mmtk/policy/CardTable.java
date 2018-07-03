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
import org.vmmagic.unboxed.Offset;

@Uninterruptible
public class CardTable {
  static final int TOTAL_CARDS;
  static final int HOTNESS_TABLE_PAGES;
  static final byte HOTNESS_THRESHOLD = 4;
  static final int[] cardTable;
  static int dirtyCards = 0;
  static final Lock dirtyCardsLock = VM.newLock("dirtyCardsLock");
  static Address cardHotnessTable = Address.zero();

  static int maxHotness = 0;

  static {
    int memorySize = VM.HEAP_END.diff(VM.HEAP_START).toInt();
    TOTAL_CARDS = memorySize >> Region.Card.LOG_BYTES_IN_CARD;
    cardTable = new int[TOTAL_CARDS >> Constants.LOG_BITS_IN_INT];
    HOTNESS_TABLE_PAGES = RemSet.ceilDiv(TOTAL_CARDS, Constants.BYTES_IN_PAGE);
  }

  @Inline
  @Uninterruptible
  public static boolean increaseHotness(Address card) {
    if (cardHotnessTable.isZero()) cardHotnessTable = Plan.metaDataSpace.acquire(HOTNESS_TABLE_PAGES);
    int cardIndex = hash(card);
    Address hotnessPtr = cardHotnessTable.plus(cardIndex);
    byte oldHotness = hotnessPtr.loadByte();
    if (oldHotness > HOTNESS_THRESHOLD) return true; // This is a hot card
    byte newHotness = (byte) (oldHotness + 1);
    hotnessPtr.store(newHotness);
    return newHotness > HOTNESS_THRESHOLD;
  }

  @Inline
  public static void clearAllHotness() {
    if (cardHotnessTable.isZero()) return;
    VM.memory.zero(false, cardHotnessTable, Extent.fromIntZeroExtend(HOTNESS_TABLE_PAGES << Constants.LOG_BYTES_IN_PAGE));
  }

  @Inline
  public static int dirtyCardSize() {
    return dirtyCards;
  }

  @Inline
  @Uninterruptible
  private static int hash(Address card) {
    return card.diff(VM.HEAP_START).toInt() >>> Region.Card.LOG_BYTES_IN_CARD;
  }
/*
  @Inline
  public static void markCard(Address card, boolean mark) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(MarkBlock.Card.of(card)));
    int cardIndex = hash(card);
    boolean oldValue, newValue;
    do {
      oldValue = getBit(cardTable, cardIndex);
      newValue = mark;
      if (oldValue == newValue) break;
    } while (!compareAndSwapBitInBuffer(cardTable, cardIndex, oldValue, newValue));
  }
*/
  @Inline
  @Uninterruptible
  public static boolean attemptToMarkCard(Address card, boolean mark) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Region.Card.of(card)));
    int cardIndex = hash(card);
    boolean success = attemptBitInBuffer(cardTable, cardIndex, mark);
    if (success) {
      dirtyCardsLock.acquire();
      dirtyCards += mark ? 1 : -1;
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(dirtyCards >= 0 && dirtyCards < cardTable.length);
      dirtyCardsLock.release();
    }
    return success;
  }

  @Inline
  @Uninterruptible
  private static boolean attemptBitInBuffer(int[] buf, int index, boolean newBit) {
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldBit != newBit);
    int intIndex = index >>> Constants.LOG_BITS_IN_INT;
    int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(intIndex == index / 32);
      VM.assertions._assert(bitIndex == index % 32);
    }
    Offset offset = Offset.fromIntZeroExtend(intIndex << Constants.LOG_BYTES_IN_INT);
    int oldValue, newValue;
    do {
      // Get old int
      oldValue = buf[intIndex];
      // Build new int
      if (newBit) {
        newValue = oldValue | (1 << (31 - bitIndex));
      } else {
        newValue = oldValue & (~(1 << (31 - bitIndex)));
      }
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(((newValue & (1 << (31 - bitIndex))) != 0) == newBit);
        if (bitIndex != 0) {
          VM.assertions._assert((oldValue >>> (32 - bitIndex)) == (newValue >>> (32 - bitIndex)));
        }
        if (bitIndex != 31) {
          VM.assertions._assert((oldValue << (1 + bitIndex)) == (newValue << (1 + bitIndex)));
        }
      }
      if (oldValue == newValue) return false; // this bit has been set by other threads
    } while (!VM.objectModel.attemptInt(buf, offset, oldValue, newValue));
    return true;
  }

  @Inline
  private static boolean getBit(int[] buf, int index) {
    int intIndex = index >>> Constants.LOG_BITS_IN_INT;
    int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(intIndex == index / 32 && bitIndex == index % 32);
    int entry = buf[intIndex];
    return (entry & (1 << (31 - bitIndex))) != 0;
    //return ((entry << bitIndex) >> (Constants.LOG_BITS_IN_INT - 1)) > 0;
  }

  @Inline
  @Uninterruptible
  public static boolean cardIsMarked(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Region.Card.of(card)));
    int cardIndex = hash(card);
    return getBit(cardTable, cardIndex);
    //return cardTable[cardIndex] > (int) 0;
  }

  public static void assertAllCardsAreNotMarked() {
    for (int i = 0; i < cardTable.length; i++) {
      if (cardTable[i] != (int) 0) {
        for (int j = 0; j < 32; j++) {
          Address card = VM.HEAP_START.plus((i * 32 + j) << Region.Card.LOG_BYTES_IN_CARD);
          VM.assertions._assert(Region.Card.isAligned(card));
          if (cardIsMarked(card)) {
            Log.write("Card ", card);
            Log.writeln(" is marked.");
          }
        }
      }
      VM.assertions._assert(cardTable[i] == (int) 0);
    }
  }
}
