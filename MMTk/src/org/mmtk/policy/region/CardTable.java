package org.mmtk.policy.region;

import org.mmtk.utility.Atomic;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoBoundsCheck;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;
import org.vmmagic.unboxed.WordArray;

@Uninterruptible
public class CardTable {
  public static final byte HOTNESS_THRESHOLD = 4;
  private static final int[] table = new int[(Card.CARDS_IN_HEAP + 3) / 4];
  private static final byte[] hotnessTable = new byte[Card.CARDS_IN_HEAP];
  private static final Atomic.Int numDirtyCards = new Atomic.Int();

  @Inline
  public static int numDirtyCards() {
    return numDirtyCards.get();
  }

  @Inline
  @NoBoundsCheck
  public static int increaseHotness(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(card));
    int index = getIndex(card);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index >= 0 && index < Card.CARDS_IN_HEAP);
    byte oldHotness = hotnessTable[index];
    if (oldHotness > HOTNESS_THRESHOLD) return oldHotness;
    byte newHotness = (byte) (oldHotness + 1);
    hotnessTable[index] = newHotness;
    return newHotness;
  }

  @Inline
  @NoBoundsCheck
  public static void clearAllHotnessPar(int id, int workers) {
    int totalSize = (Card.CARDS_IN_HEAP + workers - 1) / workers;
    int start = totalSize * id;
    int _limit = totalSize * (id + 1);
    int limit = _limit > Card.CARDS_IN_HEAP ? Card.CARDS_IN_HEAP : _limit;
    for (int i = start; i < limit; i++)
      hotnessTable[i] = 0;
  }

  public static void clear() {
    for (int i = 0; i < table.length; i++)
      table[i] = 0;
  }

  @Inline
  private static int getIndex(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(card));
    return card.diff(VM.HEAP_START).toWord().rshl(Card.LOG_BYTES_IN_CARD).toInt();
  }

  static final Word BYTE_MASK = Word.fromIntZeroExtend((1 << Constants.BITS_IN_BYTE) - 1);

  @Inline
  @NoBoundsCheck
  public static byte get(Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      if (!Card.isAligned(card)) Log.writeln("Card is not aligned ", card);
      VM.assertions._assert(Card.isAligned(card));
    }
    int index = getIndex(card);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index >= 0 && index < Card.CARDS_IN_HEAP);
    Address wordSlot = ObjectReference.fromObject(table).toAddress().plus(index & ~3);
    int shift = (index & 3) << Constants.LOG_BITS_IN_BYTE;
    Word word = wordSlot.loadWord();
    return (byte) word.and(BYTE_MASK.lsh(shift)).rshl(shift).toInt();
  }

  @Inline
  private static boolean attemptByteInWord(Address wordSlot, int byteIndex, final byte value) {
    final int shift = byteIndex << Constants.LOG_BITS_IN_BYTE;
    Word oldWord, newWord;
    do {
      oldWord = wordSlot.prepareWord();
      if (value == (byte) oldWord.and(BYTE_MASK.lsh(shift)).rshl(shift).toInt()) {
        return false;
      }
      // Clear byte
      newWord = oldWord.and(BYTE_MASK.lsh(shift).not());
      // Set new byte
      newWord = newWord.or(Word.fromIntZeroExtend(value).lsh(shift));
    } while (!wordSlot.attempt(oldWord, newWord));
    return true;
  }

  @Inline
  @NoBoundsCheck
  public static void set(Address card, final byte value) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(card));
    int index = getIndex(card);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index >= 0 && index < Card.CARDS_IN_HEAP);
    Address wordSlot = ObjectReference.fromObject(table).toAddress().plus(index & ~3);
    int shift = (index & 3) << Constants.LOG_BITS_IN_BYTE;
    boolean success = attemptByteInWord(wordSlot, index & 3, value);
    if (success) {
      if (value == Card.DIRTY) numDirtyCards.add(1);
      else numDirtyCards.add(-1);
    }
  }
}
