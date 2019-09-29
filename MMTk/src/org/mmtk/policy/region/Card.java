package org.mmtk.policy.region;

import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class Card {
  public static final int LOG_BYTES_IN_CARD = 9;
  public static final int BYTES_IN_CARD = 1 << LOG_BYTES_IN_CARD;
  public static final int CARDS_IN_HEAP = VM.HEAP_END.diff(VM.HEAP_START).toWord().rshl(LOG_BYTES_IN_CARD).toInt();
  public static final Word CARD_MASK = Word.fromIntZeroExtend(BYTES_IN_CARD - 1);// 0..0111111111

  public static final byte NOT_DIRTY = 0;
  public static final byte DIRTY = 1;

  @Inline
  public static Address of(Address address) {
    return address.toWord().and(CARD_MASK.not()).toAddress();
  }

  @Inline
  public static Address of(ObjectReference ref) {
    return of(VM.objectModel.objectStartRef(ref));
  }

  @Inline
  public static boolean isAligned(Address card) {
    return of(card).EQ(card);
  }

  @Inline
  public static int indexOf(Address card) {
    Address region = Region.of(card);
    int index = card.diff(region).toInt() >>> LOG_BYTES_IN_CARD;
    return index;
  }
}
