package org.mmtk.policy.region;

import org.mmtk.plan.Plan;
import org.mmtk.plan.g1.G1;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class RemSet {
  private static final int LOGICAL_REGIONS_IN_HEAP = VM.HEAP_END.diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_REGION).toInt();
  private static final int BYTES_IN_REMSET = LOGICAL_REGIONS_IN_HEAP * Constants.BYTES_IN_ADDRESS;
  public static final int PAGES_IN_REMSET = 1 + ((BYTES_IN_REMSET - 1) / Constants.BYTES_IN_PAGE);
  private static final int BYTES_IN_PRT = Card.CARDS_IN_REGION / Constants.BITS_IN_BYTE;
  private static final int WORDS_IN_PRT = BYTES_IN_PRT / Constants.BYTES_IN_WORD;
  private static final int PAGES_IN_PRT = 1 + ((BYTES_IN_PRT - 1) / Constants.BYTES_IN_PAGE);

  @Uninterruptible
  public static abstract class Visitor<T> {
    public abstract void visit(Address remset, Address card, T context);
  }

  public static void iterate(Address remset, Visitor visitor, Object context) {
//    VM.assertions._assert(false, "Unimplemented");
    for (Address region = VM.HEAP_START; region.LT(VM.HEAP_END); region = region.plus(Region.BYTES_IN_REGION)) {
      int regionIndex = (region.toInt() - VM.HEAP_START.toInt()) >>> Region.LOG_BYTES_IN_REGION;
      Address prtSlot = remset.plus(regionIndex << Constants.LOG_BYTES_IN_ADDRESS);
      Address prt = prtSlot.loadAddress();
      if (prt.isZero()) continue;
      if (Space.isInSpace(G1.REGION_SPACE, region)) {
         if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.getBool(region, Region.MD_ALLOCATED));
         if (Region.getBool(region, Region.MD_RELOCATE)) continue;
      }
      Address regionEnd = region.plus(Region.BYTES_IN_REGION);
      for (Address card = region; card.LT(regionEnd); card = card.plus(Card.BYTES_IN_CARD)) {
        if (PerRegionTable.containsCard(prt, card)) {
          visitor.visit(remset, card, context);
        }
      }
    }
  }

  @Inline
  private static Address getPRT(Address remset, Address region, boolean create) {
    Address slot = remset.plus(Region.heapIndexOf(region) << Constants.LOG_BYTES_IN_ADDRESS);
    Address oldValue, newValue = Address.zero();
    do {
      oldValue = slot.prepareAddress();
      if (!create) return oldValue;
      if (!oldValue.isZero()) {
        if (!newValue.isZero()) Plan.metaDataSpace.release(newValue);
        return oldValue;
      }
      if (newValue.isZero()) newValue = Plan.metaDataSpace.acquire(PAGES_IN_PRT);
    } while (!slot.attempt(oldValue, newValue));
    return newValue;
  }

  @Inline
  public static void releasePRTs(Address remset) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!remset.isZero());
    Address cursor = remset;
    final Address limit = cursor.plus(BYTES_IN_REMSET);
    while (cursor.LT(limit)) {
      Address prt = cursor.loadAddress();
      if (!prt.isZero()) Plan.metaDataSpace.release(prt);
      cursor = cursor.plus(Constants.BYTES_IN_ADDRESS);
    }
  }

  @Inline
  public static void addCard(Address remset, Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!remset.isZero());
      VM.assertions._assert(Space.isInSpace(Plan.META, remset));
      VM.assertions._assert(Card.isAligned(card));
    }
    Address region = Region.of(card);
    Address prt = getPRT(remset, region, true);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!prt.isZero());
    PerRegionTable.addCard(prt, card);
  }

  @Inline
  public static boolean containsCard(Address remset, Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!remset.isZero());
      VM.assertions._assert(Space.isInSpace(Plan.META, remset));
      VM.assertions._assert(Card.isAligned(card));
    }
    Address region = Region.of(card);
    Address prt = getPRT(remset, region, false);
    if (prt.isZero()) return false;
    return PerRegionTable.containsCard(prt, card);
  }

  @Inline
  public static void removeCard(Address remset, Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!remset.isZero());
      VM.assertions._assert(Space.isInSpace(Plan.META, remset));
      VM.assertions._assert(Card.isAligned(card));
    }
    Address region = Region.of(card);
    Address prt = getPRT(remset, region, false);
    if (!prt.isZero()) PerRegionTable.removeCard(prt, card);
  }

  @Inline
  public static void clearCardsInCollectionSet(Address remset) {
    for (Address region = VM.HEAP_START; region.LT(VM.HEAP_END); region = region.plus(Region.BYTES_IN_REGION)) {
      int regionIndex = (region.toInt() - VM.HEAP_START.toInt()) >>> Region.LOG_BYTES_IN_REGION;
      Address prtSlot = remset.plus(regionIndex << Constants.LOG_BYTES_IN_ADDRESS);
      Address prt = prtSlot.loadAddress();
      if (!prt.isZero()) {
        if (Space.isInSpace(G1.REGION_SPACE, region)) {
          clearCSetPRTs(region, prt, prtSlot);
        } else if (Space.isInSpace(G1.LOS, region)) {
          clearDeadLosCards(region, prt);
        } else if (Space.isInSpace(G1.IMMORTAL, region)) {
          // Do nothing
        } else {
          Plan.metaDataSpace.release(prt);
          prtSlot.store(Address.zero());
        }
      }
    }
  }

  @Inline
  private static void clearCSetPRTs(Address region, Address prt, Address prtSlot) {
    if (Region.getBool(region, Region.MD_RELOCATE)) {
      Plan.metaDataSpace.release(prt);
      prtSlot.store(Address.zero());
    }
  }

  @Inline
  private static void clearDeadLosCards(Address region, Address prt) {
    Address regionEnd = region.plus(Region.BYTES_IN_REGION);
    for (Address card = region; card.LT(regionEnd); card = card.plus(Card.BYTES_IN_CARD)) {
      if (PerRegionTable.containsCard(prt, card)) {
        ObjectReference o = VM.objectModel.getObjectFromStartAddress(card);
        if (!G1.loSpace.isLive(o)) {
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
      return slot;
    }

    private static final Word WORD_SHIFT_MASK = Word.fromIntZeroExtend(Constants.BITS_IN_WORD).minus(Word.one());

    @Inline
    private static Word getMask(Address prt, Address card) {
      int shift = card.toWord().rshl(Card.LOG_BYTES_IN_CARD).and(WORD_SHIFT_MASK).toInt();
      return Word.one().lsh(shift);
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
