package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.Simple;
import org.mmtk.utility.Constants;
import org.mmtk.utility.SimpleHashtable;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class CardTable {
  static final int LOG_BYTES_IN_CARD = 9;
  static final int BYTES_IN_CARD = 1 << 9;
  private static final Word CARD_MASK = Word.fromIntZeroExtend(BYTES_IN_CARD - 1);// 0..0111111111

  static SimpleHashtable hashtable;

  static {
    int memorySize = VM.AVAILABLE_END.diff(VM.AVAILABLE_START).toInt();
    int totalCards = memorySize / BYTES_IN_CARD;
    int logTotalCards;
    for (logTotalCards = 0; (1 << logTotalCards) < totalCards; logTotalCards++);
    hashtable = new SimpleHashtable(Plan.metaDataSpace, logTotalCards, Extent.fromIntZeroExtend(1)) {};
  }

  public Address cardOf(Address address) {
    return address.toWord().and(CARD_MASK.not()).toAddress();
  }

  public void markCard(Address ptr, boolean mark) {
    if (!hashtable.isValid()) hashtable.acquireTable();
    hashtable.getPayloadAddress(hashtable.getEntry(ptr.toWord(), true)).store((byte) (mark ? 1 : 0));
  }

  public boolean cardIsMarked(Address ptr) {
    if (!hashtable.isValid()) hashtable.acquireTable();
    if (!hashtable.contains(ptr.toWord())) return false;
    return hashtable.getPayloadAddress(ptr.toWord()).loadByte() > 0;
  }
}
