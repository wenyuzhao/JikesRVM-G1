package org.mmtk.policy.region;

import org.mmtk.plan.Plan;
import org.mmtk.plan.g1.G1;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class RemSet {
  private static final int LOGICAL_REGIONS_IN_HEAP = VM.HEAP_END.diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_REGION).toInt();
  private static final int BYTES_IN_REMSET = LOGICAL_REGIONS_IN_HEAP * Constants.BYTES_IN_ADDRESS;
  public static final int PAGES_IN_REMSET = 1 + ((BYTES_IN_REMSET - 1) / Constants.BYTES_IN_PAGE);
  private static final int META_BYTES_IN_PRT = 3 << Constants.LOG_BYTES_IN_ADDRESS;
  private static final int BYTES_IN_PRT = Card.CARDS_IN_REGION / Constants.BITS_IN_BYTE + META_BYTES_IN_PRT;
  private static final int PAGES_IN_PRT = 1 + ((BYTES_IN_PRT - 1) / Constants.BYTES_IN_PAGE);
  private static final Offset NEXT_PRT_OFFSET = Offset.fromIntZeroExtend(0);
  private static final Offset PREV_PRT_OFFSET = Offset.fromIntZeroExtend(Constants.BYTES_IN_ADDRESS);
  private static final Offset PRT_REGION_OFFSET = Offset.fromIntZeroExtend(Constants.BYTES_IN_ADDRESS * 2);
  private static final Offset PRT_DATA_START = Offset.fromIntZeroExtend(Constants.BYTES_IN_ADDRESS * 3);

  @Uninterruptible
  public static abstract class Visitor<T> {
    @Inline
    public abstract void visit(Address region, Address remset, Address card, T context);
  }

  public static void iterate(Address rsRegion, Visitor visitor, Object context) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!rsRegion.isZero());
      VM.assertions._assert(Space.isInSpace(G1.REGION_SPACE, rsRegion));
      VM.assertions._assert(Region.isAligned(rsRegion));
    }
    Address remset = Region.getAddress(rsRegion, Region.MD_REMSET);
    Address headPRT = Region.getAddress(rsRegion, Region.MD_REMSET_HEAD_PRT);
    for (Address prt = headPRT; !prt.isZero(); prt = prt.loadAddress(NEXT_PRT_OFFSET)) {
      Address prtRegion = prt.loadAddress(PRT_REGION_OFFSET);
      if (Space.isInSpace(G1.REGION_SPACE, prtRegion)) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.getBool(prtRegion, Region.MD_ALLOCATED));
        if (Region.getBool(prtRegion, Region.MD_RELOCATE)) continue;
      }
      // Scan for cards
      final Address prtLimit = prt.plus(BYTES_IN_PRT);
      Address prtWordSlot = prt.plus(PRT_DATA_START);
      int wordIndex = 0;
      while (prtWordSlot.LT(prtLimit)) {
        Word word = prtWordSlot.loadWord();
        if (!word.isZero()) {
          for (int i = 0; i < Constants.BITS_IN_WORD; i++) {
            if (!word.and(Word.one().lsh(i)).isZero()) {
              int bitIndex = (wordIndex << Constants.LOG_BITS_IN_WORD) + i;
              Address card = prtRegion.plus(bitIndex << Card.LOG_BYTES_IN_CARD);
              visitor.visit(rsRegion, remset, card, context);
            }
          }
        }
        prtWordSlot = prtWordSlot.plus(Constants.BYTES_IN_ADDRESS);
        wordIndex += 1;
      }
    }
  }

  @Inline
  private static Address getPRT(Address rsRegion, Address prtRegion, boolean create) {
    Address remset = Region.getAddress(rsRegion, Region.MD_REMSET);
    Address slot = remset.plus(Region.heapIndexOf(prtRegion) << Constants.LOG_BYTES_IN_ADDRESS);
    Address oldValue, newValue = Address.zero();
    do {
      oldValue = slot.prepareAddress();
      if (!create) return oldValue;
      if (!oldValue.isZero()) {
        if (!newValue.isZero()) Plan.metaDataSpace.release(newValue);
        return oldValue;
      }
      if (newValue.isZero()) {
        newValue = Plan.metaDataSpace.acquire(PAGES_IN_PRT);
        newValue.store(prtRegion, PRT_REGION_OFFSET);
      }
    } while (!slot.attempt(oldValue, newValue));
    // Add to PRT linked list
    final Address headPRTSlot = Region.metaSlot(rsRegion, Region.MD_REMSET_HEAD_PRT);
    Address oldHeadPRT;
    final Address newHeadPRT = newValue;
    do {
      oldHeadPRT = headPRTSlot.prepareAddress();
      newHeadPRT.store(oldHeadPRT);
    } while (!headPRTSlot.attempt(oldHeadPRT, newHeadPRT));
    if (!oldHeadPRT.isZero()) oldHeadPRT.plus(Constants.BYTES_IN_ADDRESS).store(newHeadPRT);
    return newValue;
  }

  @Inline
  public static void releasePRTs(Address headPRT) {
    while (!headPRT.isZero()) {
      Address nextPRT = headPRT.loadAddress(NEXT_PRT_OFFSET);
      Plan.metaDataSpace.release(headPRT);
      headPRT = nextPRT;
    }
  }

  @Inline
  public static void addCard(Address region, Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!region.isZero());
      VM.assertions._assert(Space.isInSpace(G1.REGION_SPACE, region));
      VM.assertions._assert(Region.isAligned(region));
      VM.assertions._assert(Card.isAligned(card));
    }
    Address cardRegion = Region.of(card);
    Address prt = getPRT(region, cardRegion, true);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!prt.isZero());
    PerRegionTable.addCard(prt, card);
  }

  @Inline
  public static void removeCard(Address region, Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!region.isZero());
      VM.assertions._assert(Space.isInSpace(G1.REGION_SPACE, region));
      VM.assertions._assert(Region.isAligned(region));
      VM.assertions._assert(Card.isAligned(card));
    }
    Address cardRegion = Region.of(card);
    Address prt = getPRT(region, cardRegion, false);
    if (!prt.isZero()) PerRegionTable.removeCard(prt, card);
  }

  @Inline
  public static void clearCardsInCollectionSet(Address rsRegion) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!rsRegion.isZero());
      VM.assertions._assert(Space.isInSpace(G1.REGION_SPACE, rsRegion));
      VM.assertions._assert(Region.isAligned(rsRegion));
    }
    Address remset = Region.getAddress(rsRegion, Region.MD_REMSET);
    Address prt = Region.getAddress(rsRegion, Region.MD_REMSET_HEAD_PRT);
    while (!prt.isZero()) {
      Address nextPRT = prt.loadAddress(NEXT_PRT_OFFSET);

      Address prtRegion = prt.loadAddress(PRT_REGION_OFFSET);
      int regionIndex = (prtRegion.toInt() - VM.HEAP_START.toInt()) >>> Region.LOG_BYTES_IN_REGION;
      Address prtSlot = remset.plus(regionIndex << Constants.LOG_BYTES_IN_ADDRESS);
      if (Space.isInSpace(G1.REGION_SPACE, prtRegion)) {
        clearCSetPRTs(rsRegion, prtRegion, prt, prtSlot);
      } else if (Space.isInSpace(G1.LOS, prtRegion)) {
        clearDeadLosCards(prtRegion, prt);
      } else if (Space.isInSpace(G1.IMMORTAL, prtRegion)) {
        // Do nothing
      } else {
        releaseOnePRT(
            Region.metaSlot(rsRegion, Region.MD_REMSET_HEAD_PRT),
            prtSlot,
            prt
        );
      }

      prt = nextPRT;
    }
  }

  @Inline
  private static void releaseOnePRT(Address headPRTSlot, Address prtSlot, Address prt) {
    // Remove from freelist
    Address nextPRT = prt.loadAddress(NEXT_PRT_OFFSET);
    Address prevPRT = prt.loadAddress(PREV_PRT_OFFSET);
    if (!nextPRT.isZero()) nextPRT.store(prevPRT, PREV_PRT_OFFSET);
    if (!prevPRT.isZero()) prevPRT.store(nextPRT, NEXT_PRT_OFFSET);
    if (headPRTSlot.loadAddress().EQ(prt)) headPRTSlot.store(nextPRT);
    // Remove from table
    prtSlot.store(Address.zero());
    // Release memory
    Plan.metaDataSpace.release(prt);
  }

  @Inline
  private static void clearCSetPRTs(Address rsRegion, Address prtRegion, Address prt, Address prtSlot) {
    if (Region.getBool(prtRegion, Region.MD_RELOCATE)) {
      releaseOnePRT(
          Region.metaSlot(rsRegion, Region.MD_REMSET_HEAD_PRT),
          prtSlot,
          prt
      );
    }
  }

  @Inline
  private static void clearDeadLosCards(Address region, Address prt) {
    Address regionEnd = region.plus(Region.BYTES_IN_REGION);
    for (Address card = region; card.LT(regionEnd); card = card.plus(Card.BYTES_IN_CARD)) {
      if (PerRegionTable.containsCard(prt, card)) {
        ObjectReference o = Card.getObjectFromStartAddress(card, card.plus(Card.BYTES_IN_CARD));
        if (o.isNull() || !G1.loSpace.isLive(o)) {
          PerRegionTable.removeCard(prt, card);
        }
      }
    }
  }

  @Uninterruptible
  static class PerRegionTable {
    @Inline
    private static Address getWord(Address prt, Address card) {
      Address region = Region.of(card);
      int bitIndex = card.diff(region).toWord().rshl(Card.LOG_BYTES_IN_CARD).toInt();
      int wordIndex = bitIndex >> Constants.LOG_BITS_IN_WORD;
      Address slot = prt.plus(wordIndex << Constants.LOG_BYTES_IN_WORD);
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(slot.GE(prt));
        VM.assertions._assert(slot.LT(prt.plus(BYTES_IN_PRT)));
      }
      // The first slot is used to store next PRT pointer
      return slot.plus(PRT_DATA_START);
    }

    private static final Word WORD_SHIFT_MASK = Word.fromIntZeroExtend(Constants.BITS_IN_WORD).minus(Word.one());

    @Inline
    private static Word getMask(Address prt, Address card) {
      int shift = card.toWord().rshl(Card.LOG_BYTES_IN_CARD).and(WORD_SHIFT_MASK).toInt();
      Word mask = Word.one().lsh(shift);
      if (VM.VERIFY_ASSERTIONS) {
        Address region = Region.of(card);
        int bitIndex = card.diff(region).toWord().rshl(Card.LOG_BYTES_IN_CARD).toInt();
        int shift2 = bitIndex % 32;
        VM.assertions._assert(shift == shift2);
      }
      return mask;
    }

    @Inline
    public static boolean addCard(Address prt, Address card) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!prt.isZero());
        VM.assertions._assert(Space.isInSpace(Plan.META, prt));
        VM.assertions._assert(Card.isAligned(card));
      }
      Address slot = getWord(prt, card);
      Word mask = getMask(prt, card);
      Word oldValue, newValue;
      do {
        oldValue = slot.prepareWord();
        if (oldValue.and(mask).EQ(mask)) return false;
        newValue = oldValue.or(mask);
      } while (!slot.attempt(oldValue, newValue));
      return true;
    }

    @Inline
    public static boolean removeCard(Address prt, Address card) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!prt.isZero());
        VM.assertions._assert(Space.isInSpace(Plan.META, prt));
        VM.assertions._assert(Card.isAligned(card));
      }
      Address slot = getWord(prt, card);
      Word mask = getMask(prt, card);
      Word oldValue, newValue;
      do {
        oldValue = slot.prepareWord();
        if (oldValue.and(mask).isZero()) return false;
        newValue = oldValue.and(mask.not());
      } while (!slot.attempt(oldValue, newValue));
      return true;
    }

    @Inline
    public static boolean containsCard(Address prt, Address card) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!prt.isZero());
        VM.assertions._assert(Space.isInSpace(Plan.META, prt));
        VM.assertions._assert(Card.isAligned(card));
      }
      Address slot = getWord(prt, card);
      Word mask = getMask(prt, card);
      Word oldValue = slot.prepareWord();
      return oldValue.and(mask).EQ(mask);
    }
  }
}
