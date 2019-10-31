package org.mmtk.policy.region;

import org.mmtk.plan.g1.G1;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BumpPointer2;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class CardOffsetTable {
  public static final int BYTES_IN_CARD_OFFSET_TABLE = Card.CARDS_IN_REGION << Constants.LOG_BYTES_IN_ADDRESS;
  public static final int PAGES_IN_CARD_OFFSET_TABLE = (BYTES_IN_CARD_OFFSET_TABLE + (Constants.BYTES_IN_PAGE - 1)) / Constants.BYTES_IN_PAGE;

  @Inline
  public static void set(Address region, Address card, Address offset) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isInSpace(G1.REGION_SPACE, region));
    Address bot = Region.getAddress(region, Region.MD_CARD_OFFSET_TABLE);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!bot.isZero());
    int index = Card.indexOf(card);
    Address botEntry = bot.plus(index << Constants.LOG_BYTES_IN_ADDRESS);
    botEntry.store(offset);
  }

  @Inline
  private static Address get(Address bot, Address card) {
    int index = Card.indexOf(card);
    Address botEntry = bot.plus(index << Constants.LOG_BYTES_IN_ADDRESS);
    return botEntry.loadAddress();
  }

  @Inline
  public static Address blockStart(Address region, Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(card.GE(region));
      Address cardEnd = card.plus(Card.BYTES_IN_CARD);
      Address cursor = Region.getAddress(region, Region.MD_NEXT_CURSOR);
      VM.assertions._assert(cardEnd.LE(cursor));
    }
    Address bot = Region.getAddress(region, Region.MD_CARD_OFFSET_TABLE);
    Address start = get(bot, card);

    if (VM.VERIFY_ASSERTIONS) {
      if (start.isZero()) {
        Log.writeln("Incorrect offset for card ", card);
      }
      VM.assertions._assert(!start.isZero());
    }
    if (start.GE(card)) return start;
    return blockStartSlow(bot, card, start);
  }

  @Inline
  private static Address blockStartSlow(Address bot, Address card, Address blockStart) {
    Address cursor = blockStart;
    final Address limit = card.plus(Card.BYTES_IN_CARD);

    while (true) {
      Address startRef; ObjectReference object;
      {
        Address cursor2 = cursor;
        if (cursor2.GE(limit)) return Address.zero();
        while (cursor2.loadInt() == Constants.ALIGNMENT_VALUE) {
          cursor2 = cursor2.plus(Constants.BYTES_IN_WORD);
          if (cursor2.GE(limit)) return Address.zero();
        }
        object = cursor2.plus(Card.OBJECT_REF_OFFSET).toObjectReference();
        if (BumpPointer2.tibIsZero(object)) return Address.zero();
        startRef = cursor2;
      }
      if (startRef.GE(card)) {
        Address regionEnd = Region.getAddress(Region.of(card), Region.MD_NEXT_CURSOR);
        Address c = card;
        int c_index = Card.indexOf(card);
        while (c.LT(regionEnd)) {
          Address botEntry = bot.plus(c_index << Constants.LOG_BYTES_IN_ADDRESS);
          if (botEntry.loadAddress().LT(startRef)) {
            botEntry.store(startRef);
          } else {
            break;
          }
          c = c.plus(Card.BYTES_IN_CARD);
          c_index += 1;
        }
        return startRef;
      }
      cursor = VM.objectModel.getObjectEndAddress(object);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(cursor.EQ(startRef.plus(VM.objectModel.getCurrentSize(object))));
    }
  }
}
