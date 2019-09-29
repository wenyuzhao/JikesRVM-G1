package org.mmtk.policy.region;

import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoBoundsCheck;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.WordArray;

@Uninterruptible
public class CardTable {
  private static final byte[] table = new byte[Card.CARDS_IN_HEAP];

  @Inline
  private static int getIndex(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(card));
    return card.diff(VM.HEAP_START).toWord().rshl(Card.LOG_BYTES_IN_CARD).toInt();
  }

  @Inline
  @NoBoundsCheck
  public static byte get(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(card));
    int index = getIndex(card);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index >= 0 && index < Card.CARDS_IN_HEAP);
    return table[index];
  }

  @Inline
  @NoBoundsCheck
  public static void set(Address card, byte value) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(card));
    int index = getIndex(card);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index >= 0 && index < Card.CARDS_IN_HEAP);
    table[index] = value;
  }
}
