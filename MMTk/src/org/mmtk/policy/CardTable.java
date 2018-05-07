package org.mmtk.policy;

import org.mmtk.plan.markcopy.remset.MarkCopy;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Offset;

@Uninterruptible
public class CardTable {
  static final int[] cardTable;

  static {
    int memorySize = VM.HEAP_END.diff(VM.HEAP_START).toInt();
    int totalCards = memorySize >> MarkBlock.Card.LOG_BYTES_IN_CARD;
    cardTable = new int[totalCards >> Constants.LOG_BITS_IN_INT];
  }


  @Inline
  private static int hash(Address card) {
    return card.diff(VM.HEAP_START).toInt() >> MarkBlock.Card.LOG_BYTES_IN_CARD;
  }

  @Inline
  public static void markCard(Address card, boolean mark) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(MarkBlock.Card.of(card)));
    int cardIndex = hash(card);
    boolean oldValue, newValue;
    do {
      oldValue = getBit(cardTable, cardIndex);
      newValue = mark;
      if (oldValue == newValue) break;
    } while (compareAndSwapBitInBuffer(cardTable, cardIndex, oldValue, newValue));
  }

  @Inline
  public static boolean attemptToMarkCard(Address card, boolean mark) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(MarkBlock.Card.of(card)));
    int cardIndex = hash(card);
    boolean oldValue = getBit(cardTable, cardIndex);
    boolean newValue = mark;
    if (oldValue == newValue) return false; // card is already marked by other threads
    return compareAndSwapBitInBuffer(cardTable, cardIndex, oldValue, newValue);
  }

  @Inline
  public static boolean compareAndSwapBitInBuffer(int[] buf, int index, boolean oldBit, boolean newBit) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldBit != newBit);
    int intIndex = index >> Constants.LOG_BITS_IN_INT;
    int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(intIndex >= 0 && intIndex < buf.length);
      VM.assertions._assert(bitIndex >= 0 && bitIndex < Constants.BITS_IN_INT);
    }
    Offset offset = Offset.fromIntZeroExtend(intIndex << Constants.LOG_BYTES_IN_INT);
    // Get old int
    int oldValue = buf[intIndex];
    // Build new int
    int newValue = oldValue;
    if (newBit) {
      newValue &= ~(1 << (31 - bitIndex));
    } else {
      newValue |= 1 << (31 - bitIndex);
    }
    //int newValue = oldValue & ~(1 << ); // Drop the target bit
    //newValue |= (newBit ? 1 : 0) << (31 - bitIndex); // Set new byte

    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert((newValue << (bitIndex + 1)) == (oldValue << (bitIndex + 1)) && (newValue >>> (32 - bitIndex)) == (oldValue >>> (32 - bitIndex)));
    }
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldValue != newValue);
    if (oldValue == newValue) return true;
    return VM.objectModel.attemptInt(buf, offset, oldValue, newValue);
  }

  @Inline
  public static boolean getBit(int[] buf, int index) {
    int intIndex = index >> Constants.LOG_BITS_IN_INT;
    int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
    int entry = buf[intIndex];
    return ((entry << bitIndex) >> (Constants.LOG_BITS_IN_INT - 1)) > 0;
  }

  @Inline
  public static boolean cardIsMarked(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(MarkBlock.Card.of(card)));
    int cardIndex = hash(card);
    return getBit(cardTable, cardIndex);
    //return cardTable[cardIndex] > (int) 0;
  }

  public static void assertAllCardsAreNotMarked() {
    for (int i = 0; i < cardTable.length; i++) {
      if (cardTable[i] != (int) 0) {
        Address card = VM.HEAP_START.plus(i << MarkBlock.Card.LOG_BYTES_IN_CARD);
        VM.assertions._assert(MarkBlock.Card.isAligned(card));
        VM.assertions._assert(cardIsMarked(card));
        Log.write("Card ", card);
        Log.writeln(" is marked.");
      }
      VM.assertions._assert(cardTable[i] == (int) 0);
    }
  }
}
