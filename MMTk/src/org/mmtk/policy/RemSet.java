package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Conversions;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BlockAllocator;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class RemSet {
  private final static AddressArray rememberedSets; // Array<RemSet: Array<PRT>>
  public final static int TOTAL_REGIONS;
  public final static int REMSET_PAGES;
  public final static int MAX_CARDS_PER_REGION;
  private final static int INTS_IN_PRT;

  static {
    if (Region.USE_CARDS) {
      Word heapSize = VM.HEAP_END.diff(VM.HEAP_START).toWord();
      TOTAL_REGIONS = heapSize.rshl(Region.LOG_BYTES_IN_REGION).toInt();
      rememberedSets = AddressArray.create(TOTAL_REGIONS);//new int[TOTAL_REGIONS][][];
      REMSET_PAGES = ceilDiv(TOTAL_REGIONS << Constants.LOG_BYTES_IN_ADDRESS, Constants.BYTES_IN_PAGE);
      int cardsPerRegion = Region.BYTES_IN_REGION >>> Region.Card.LOG_BYTES_IN_CARD;
      MAX_CARDS_PER_REGION = cardsPerRegion;
      int bytesInPRT = cardsPerRegion >>> Constants.LOG_BITS_IN_BYTE;
      INTS_IN_PRT = ceilDiv(bytesInPRT, Constants.BYTES_IN_INT);
//      PAGES_IN_PRT = ceilDiv(bytesInPRT, Constants.BYTES_IN_PAGE);
    } else {
      rememberedSets = null;
      TOTAL_REGIONS = 0;
      REMSET_PAGES = 0;
      MAX_CARDS_PER_REGION = 0;
//      PAGES_IN_PRT = 0;
      INTS_IN_PRT = 0;
    }
  }

  @NoInline
  public static int calculateRemSetPages() {
    return PerRegionTable.memoryPool.pages();
  }

  @Uninterruptible
  private static class PerRegionTable {
    static final MemoryPool memoryPool = new MemoryPool(Region.BYTES_IN_REGION / Region.Card.BYTES_IN_CARD / BITS_IN_BYTE);

    @Inline
    static Address alloc() {
      return memoryPool.alloc();
    }

    @Inline
    static void free(Address a) {
      memoryPool.free(a);
    }

    @Inline
    private static boolean attemptBitInBuffer(Address buf, int index, boolean newBit) {
      int intIndex = index >>> Constants.LOG_BITS_IN_INT;
      int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
      Offset offset = Offset.fromIntZeroExtend(intIndex << Constants.LOG_BYTES_IN_INT);
      Address slot = buf.plus(offset);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(pointer.LT(buf.plus(Constants.BYTES_IN_PAGE * PAGES_IN_PRT)));
      int oldValue, newValue;
      do {
        // Get old int
        oldValue = slot.prepareInt();//buf[intIndex];
        boolean oldBit = (oldValue & (1 << (31 - bitIndex))) != 0;
        if (oldBit == newBit) return false;
        // Build new int
        if (newBit) {
          newValue = oldValue | (1 << (31 - bitIndex));
        } else {
          newValue = oldValue & (~(1 << (31 - bitIndex)));
        }
        if (oldValue == newValue) return false; // this bit has been set by other threads
      } while (!slot.attempt(oldValue, newValue));
      return true;
    }

    @Inline
    static boolean getBit(Address buf, int index) {
      int intIndex = index >>> Constants.LOG_BITS_IN_INT;
      int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
      int entry = buf.plus(intIndex << Constants.LOG_BYTES_IN_INT).loadInt();//buf[intIndex];
      return (entry & (1 << (31 - bitIndex))) != 0;
    }

    @Inline
    static boolean contains(Address prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >>> Region.Card.LOG_BYTES_IN_CARD;
      return getBit(prt, index);
    }

    @Inline
    static boolean insert(Address prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >>> Region.Card.LOG_BYTES_IN_CARD;
      return attemptBitInBuffer(prt, index, true);
    }

    @Inline
    static boolean remove(Address prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >>> Region.Card.LOG_BYTES_IN_CARD;
      return attemptBitInBuffer(prt, index, false);
    }
  }

  @Inline
  private static void lock(Address region) {
    Address remSetLock = Region.metaDataOf(region, Region.METADATA_REMSET_LOCK_OFFSET);
    do {
      if (VM.VERIFY_ASSERTIONS) {
        int oldValue = remSetLock.prepareInt();
        VM.assertions._assert(oldValue == 0 || oldValue == 1);
      }
    } while (!remSetLock.attempt(0, 1));
  }

  @Inline
  private static void unlock(Address region) {
    Address remSetLock = Region.metaDataOf(region, Region.METADATA_REMSET_LOCK_OFFSET);
    remSetLock.store(0);
  }

  @Inline
  public static void addCardNoCheck(int g1Space, ObjectReference src, ObjectReference ref) {
    if (Space.isInSpace(g1Space, ref)) {
      Address card = Region.Card.of(src);
      if (card.GE(VM.AVAILABLE_START)) {
        Address region = Region.of(ref);
//        if (region.NE(Region.of(src)))
        RemSet.addCard(region, card);
      }
    }
  }

  @NoInline
  private static void addToRemSet(int g1Space, ObjectReference src, ObjectReference ref) {
    addCardNoCheck(g1Space, src, ref);
  }

  @Inline
  public static void updateEdge(int g1Space, ObjectReference src, ObjectReference ref) {
    Word x = VM.objectModel.refToAddress(src).toWord();
    Word y = VM.objectModel.refToAddress(ref).toWord();
    Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
    tmp = ref.isNull() ? Word.zero() : tmp;
    if (!tmp.isZero()) {
      addToRemSet(g1Space, src, ref);
    }
  }


  /** Get PRT of remset of the `region` that contains `card` */
  @Inline
  private static Address preparePRT(Address region, Address card, boolean create) {
    // Get region index
    int regionIndex = region.diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_REGION).toInt();
    // Get foreign region index
    int foreignRegionIndex = Region.of(card).diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_REGION).toInt();
    // Get PerRegionTable list, this is a page size
    Address prtList = rememberedSets.get(regionIndex);//rememberedSets[regionIndex];
//    Address remsetPagesSlot = Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET);
    if (prtList.isZero()) { // create remset
      if (create) {
        prtList = Plan.metaDataSpace.acquire(REMSET_PAGES);
        rememberedSets.set(regionIndex, prtList);
//        remsetPagesSlot.store(remsetPagesSlot.loadInt() + REMSET_PAGES);
      } else {
        return Address.zero();
      }
    }
    // Insert PerRegionTable if necessary
    Address prtEntry = prtList.plus(foreignRegionIndex << Constants.LOG_BYTES_IN_ADDRESS);
    if (VM.VERIFY_ASSERTIONS)
      VM.assertions._assert(prtEntry.LT(prtList.plus(Constants.BYTES_IN_PAGE * REMSET_PAGES)));
    if (create && prtEntry.loadAddress().isZero()) {
      prtEntry.store(PerRegionTable.alloc());
    }
    // Get PerRegionTable
    return prtEntry.loadAddress();
  }

  @Inline
  public static void addCard(Address region, Address card) {
    lock(region);

    Address prt = preparePRT(region, card, true);
    // Insert card into the target PerRegionTable
    if (PerRegionTable.insert(prt, card)) {
      // Increase REMSET_SIZE
      Address sizePointer = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET);
      int oldSize, newSize;
      do {
        oldSize = sizePointer.prepareInt();
        newSize = oldSize + 1;
//        if (oldSize == MAX_CARDS_PER_REGION) break;
      } while (!sizePointer.attempt(oldSize, newSize));
    }

    unlock(region);
  }

  @Inline
  public static boolean contains(Address region, Address card) {
    Address prt = preparePRT(region, card, false);
    if (prt.isZero()) return false;
    return PerRegionTable.contains(prt, card);
  }

  @Inline
  public static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  @Uninterruptible
  static public abstract class RemSetCardScanningTimer {
    @Inline
    public abstract void updateRemSetCardScanningTime(long time, long cards);
  }

  @Uninterruptible
  public static class Processor {
    RegionSpace regionSpace;
    TraceLocal redirectPointerTrace;
    final boolean nursery;

    public Processor(TraceLocal redirectPointerTrace, RegionSpace regionSpace, boolean nursery) {
      this.redirectPointerTrace = redirectPointerTrace;
      this.regionSpace = regionSpace;
      this.nursery = nursery;
    }

    TransitiveClosure scanEdge = new TransitiveClosure() {
      @Override @Uninterruptible
      public void processEdge(ObjectReference source, Address slot) {
        ObjectReference ref = slot.loadObjectReference();
        final int g1Space = regionSpace.getDescriptor();
        if (!ref.isNull() && Space.isInSpace(g1Space, ref) && Region.relocationRequired(Region.of(ref))) {
          ObjectReference newRef = redirectPointerTrace.traceObject(ref);
          slot.store(newRef);
//          if (newRef.toAddress().NE(ref.toAddress()))
          RemSet.updateEdge(g1Space, source, newRef);
//          if (!ref.isNull() && Space.isInSpace(regionSpace.getDescriptor(), ref)) {
//            Address region = Region.of(ref);
//            if (region.NE(Region.of(source))) {
//              Address card = Region.Card.of(source);
//              RemSet.addCard(region, card);
//            }
//          }
        }
      }
    };

    LinearScan cardLinearScan = new LinearScan() {
      @Override @Uninterruptible @Inline public void scan(ObjectReference object) {
        if (redirectPointerTrace.isLive(object)) {
          VM.scanning.scanObject(scanEdge, object);
        }
      }
    };

    /** Scan all cards in remsets of collection regions */
    @Inline
    public void processRemSets(AddressArray relocationSet, boolean concurrent, boolean _nursery, RegionSpace regionSpace, RemSetCardScanningTimer remSetCardScanningTimer) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int regionsToVisit = ceilDiv(relocationSet.length(), workers);
      final int REGION_SPACE = regionSpace.getDescriptor();
      long totalTime = 0, totalCards = 0;

      for (int i = 0; i < regionsToVisit; i++) {
        //int cursor = regionsToVisit * id + i;
        int cursor = i * workers + id;
        if (cursor >= relocationSet.length()) continue;
        Address region = relocationSet.get(cursor);
        if (region.isZero()) continue;
//        final int totalRemSetSize = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
//        if (totalRemSetSize == 0) continue;
        //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!region.isZero());
        // Iterate all its PRTs
        int regionIndex = region.diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_REGION).toInt();
        Address prtList = rememberedSets.get(regionIndex);
        if (prtList.isZero()) continue;
        for (int j = 0; j < TOTAL_REGIONS; j++) {
          Address prt = prtList.plus(j << Constants.LOG_BYTES_IN_ADDRESS).loadAddress();
          if (prt.isZero()) continue;
          Address currentRegion = VM.HEAP_START.plus(j << Region.LOG_BYTES_IN_REGION);
          // Iterate all entries in prt
          for (int k = 0; k < INTS_IN_PRT; k++) {
            if (prt.plus(k << Constants.LOG_BYTES_IN_INT).loadInt() == 0) continue;
            int cardIndexStart = k << Constants.LOG_BITS_IN_INT;
            int cardIndexEnd = cardIndexStart + Constants.BITS_IN_INT;

            for (int cardIndex = cardIndexStart; cardIndex < cardIndexEnd; cardIndex++) {
              if (!PerRegionTable.getBit(prt, cardIndex)) continue;
              Address card = currentRegion.plus(cardIndex << Region.Card.LOG_BYTES_IN_CARD);
              // This `card` is in rem-set of `region`
//              if (!Space.isMappedAddress(card)) continue;
//              if (card.LT(VM.AVAILABLE_START)) continue;
//              if (Space.isInSpace(Plan.VM_SPACE, card)) continue;
//              if (Space.isInSpace(Plan.META, card)) continue;
              if (Space.isInSpace(REGION_SPACE, card)) {
                Address regionOfCard = Region.of(card);
                if (!Region.allocated(regionOfCard) || Region.relocationRequired(regionOfCard)) {
                  continue;
                }
//                if (Region.of(card).EQ(EmbeddedMetaData.getMetaDataBase(card))) continue;
              }

              long time = VM.statistics.nanoTime();
              Region.Card.linearScan(cardLinearScan, regionSpace, card, false);
              totalTime += (VM.statistics.nanoTime() - time);
              totalCards += 1;
            }
          }
        }
      }

      remSetCardScanningTimer.updateRemSetCardScanningTime(totalTime, totalCards);
    }
  }

  /** Remove cards in collection regions from remsets of other regions & Release remsets of collection regions */
  @Inline
  public static void cleanupRemSetRefsToRelocationSet(RegionSpace regionSpace, AddressArray relocationSet, boolean emptyRegionsOnly) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().rendezvous();
    int regionsToVisit = ceilDiv(TOTAL_REGIONS, workers);

    for (int i = 0; i < regionsToVisit; i++) {
      int cursor = regionsToVisit * id + i;
      if (cursor >= TOTAL_REGIONS) break;
      Address visitedRegion = VM.HEAP_START.plus(cursor << Region.LOG_BYTES_IN_REGION);
      // If this is a relocation region, clear its rem-sets
      if (Space.isInSpace(regionSpace.getDescriptor(), visitedRegion)
          && visitedRegion.NE(EmbeddedMetaData.getMetaDataBase(visitedRegion))
          && Region.relocationRequired(visitedRegion)
          && (!emptyRegionsOnly || (emptyRegionsOnly && Region.usedSize(visitedRegion) == 0))
      ) {
        Address prtList = rememberedSets.get(cursor);
        if (!prtList.isZero()) {
          Address prtPrtEnd = prtList.plus(REMSET_PAGES << Constants.LOG_BYTES_IN_PAGE);
          for (Address prtPtr = prtList; prtPtr.LT(prtPrtEnd); prtPtr = prtPtr.plus(BYTES_IN_ADDRESS)) {
            if (VM.VERIFY_ASSERTIONS)
              VM.assertions._assert(prtPtr.LT(prtList.plus(Constants.BYTES_IN_PAGE * REMSET_PAGES)));
            Address prt = prtPtr.loadAddress();
            if (!prt.isZero()) {
              PerRegionTable.free(prt);
            }
          }
          Plan.metaDataSpace.release(prtList);
          rememberedSets.set(cursor, Address.zero());
        }
        continue;
      }
      // Else, clear all PRT corresponds to CSet
      Address prtList = rememberedSets.get(cursor);
      if (prtList.isZero()) continue;
      Address cardsSlot = Region.metaDataOf(visitedRegion, Region.METADATA_REMSET_SIZE_OFFSET);
      for (int j = 0; j < relocationSet.length(); j++) {
        Address cRegion = relocationSet.get(j);
        if (cRegion.isZero()) continue;
        if (emptyRegionsOnly && Region.usedSize(cRegion) != 0) continue;
        int index = cRegion.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION;
        Address prtSlot = prtList.plus(index << Constants.LOG_BYTES_IN_ADDRESS);
        Address prt = prtSlot.loadAddress();
        if (!prtSlot.loadAddress().isZero()) {
          // Decrease cards count
          // Iterate all entries in prt
          int cards = 0;
          for (int k = 0; k < INTS_IN_PRT; k++) {
            if (prt.plus(k << Constants.LOG_BYTES_IN_INT).loadInt() == 0) continue;
            int cardIndexStart = k << Constants.LOG_BITS_IN_INT;
            int cardIndexEnd = cardIndexStart + Constants.BITS_IN_INT;
            for (int cardIndex = cardIndexStart; cardIndex < cardIndexEnd; cardIndex++) {
              if (PerRegionTable.getBit(prt, cardIndex)) {
                cards += 1;
              }
            }
          }
          int oldCards = cardsSlot.loadInt();
          int newCards = oldCards - cards;
          newCards = newCards > 0 ? newCards : 1;
          if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isInSpace(regionSpace.getDescriptor(), cardsSlot));
          cardsSlot.store(newCards);

          PerRegionTable.free(prt);
//          Address remsetPagesSlot = Region.metaDataOf(visitedRegion, Region.METADATA_REMSET_PAGES_OFFSET);
//          int n = remsetPagesSlot.loadInt() - PAGES_IN_PRT;
//          remsetPagesSlot.store(n > 0 ? n : 0);
          prtSlot.store(Address.zero());
        }
      }
    }

    VM.activePlan.collector().rendezvous();
  }
}
