package org.mmtk.policy;

import org.mmtk.utility.Constants;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;

@Uninterruptible
public class CardTable {
  static final byte[] cardTable;

  static {
    int memorySize = VM.HEAP_END.diff(VM.HEAP_START).toInt();
    int totalCards = memorySize >> MarkBlock.Card.LOG_BYTES_IN_CARD;
    cardTable = new byte[totalCards];
  }


  @Inline
  private static int hash(Address card) {
    return card.diff(VM.HEAP_START).toInt() >> MarkBlock.Card.LOG_BYTES_IN_CARD;
  }

  @Inline
  public static void markCard(Address card, boolean mark) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(MarkBlock.Card.of(card)));
    int cardIndex = hash(card);
    cardTable[cardIndex] = (byte) (mark ? 1 : 0);
  }

  @Inline
  public static boolean cardIsMarked(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(MarkBlock.Card.of(card)));
    int cardIndex = hash(card);
    return cardTable[cardIndex] > (byte) 0;
  }
}
