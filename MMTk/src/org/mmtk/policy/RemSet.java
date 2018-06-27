package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.Scanning;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

@Uninterruptible
public class RemSet {
  private static Space space = Plan.metaDataSpace;
  private static AddressArray rememberedSets; // Array<RemSet: Array<PRT>>
  // private final static int PER_REGION_TABLE_BYTES;
  private final static int TOTAL_REGIONS;
  private final static int REMSET_PAGES;
  private final static int PAGES_IN_PRT;
  private final static int INTS_IN_PRT;

  static {
    Word heapSize = VM.HEAP_END.diff(VM.HEAP_START).toWord();
    TOTAL_REGIONS = heapSize.rshl(Region.LOG_BYTES_IN_BLOCK).toInt();
    rememberedSets = AddressArray.create(TOTAL_REGIONS);//new int[TOTAL_REGIONS][][];
    REMSET_PAGES = ceilDiv(TOTAL_REGIONS << Constants.LOG_BYTES_IN_ADDRESS, Constants.BYTES_IN_PAGE);
    int cardsPerRegion = Region.BYTES_IN_BLOCK >> Region.Card.LOG_BYTES_IN_CARD;
    int bytesInPRT = cardsPerRegion >> Constants.LOG_BITS_IN_BYTE;
    INTS_IN_PRT = ceilDiv(bytesInPRT, Constants.BYTES_IN_INT);
    PAGES_IN_PRT = ceilDiv(bytesInPRT, Constants.BYTES_IN_PAGE);
  }

  @Uninterruptible
  private static class PerRegionTable {
    @Inline
    private static boolean attemptBitInBuffer(Address buf, int index, boolean newBit) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldBit != newBit);
      int intIndex = index >> Constants.LOG_BITS_IN_INT;
      int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
      if (VM.VERIFY_ASSERTIONS) {
        //VM.assertions._assert(intIndex >= 0 && intIndex < buf.length);
        VM.assertions._assert(bitIndex >= 0 && bitIndex < Constants.BITS_IN_INT);
      }
      Offset offset = Offset.fromIntZeroExtend(intIndex << Constants.LOG_BYTES_IN_INT);
      int oldValue, newValue;
      do {
        // Get old int
        oldValue = buf.plus(intIndex << Constants.LOG_BYTES_IN_INT).loadInt();//buf[intIndex];
        // Build new int
        if (newBit) {
          newValue = oldValue | (1 << (31 - bitIndex));
        } else {
          newValue = oldValue & (~(1 << (31 - bitIndex)));
        }
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(((newValue & (1 << (31 - bitIndex))) != 0) == newBit);
          if (bitIndex != 0) {
            VM.assertions._assert((oldValue >> (32 - bitIndex)) == (newValue >> (32 - bitIndex)));
          }
          if (bitIndex != 31) {
            VM.assertions._assert((oldValue << (1 + bitIndex)) == (newValue << (1 + bitIndex)));
          }
        }
        if (oldValue == newValue) return false; // this bit has been set by other threads
      } while (!VM.objectModel.attemptInt(buf, offset, oldValue, newValue));
      return true;
    }

    @Inline
    static boolean getBit(Address buf, int index) {
      int intIndex = index >> Constants.LOG_BITS_IN_INT;
      int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
      int entry = buf.plus(intIndex << Constants.LOG_BYTES_IN_INT).loadInt();//buf[intIndex];
      return (entry & (1 << (31 - bitIndex))) != 0;
    }

    @Inline
    static boolean contains(Address prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
      return getBit(prt, index);
    }

    @Inline
    static boolean insert(Address prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
      //Address ptr = ObjectReference.fromObject(prt).toAddress().plus(index << Constants.LOG_BYTES_IN_INT);
      return attemptBitInBuffer(prt, index, true);
    }

    @Inline
    static boolean remove(Address prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
      return attemptBitInBuffer(prt, index, false);
    }
  }

  @Inline
  private static void lock(Address region) {
    Address remSetLock = Region.metaDataOf(region, Region.METADATA_REMSET_LOCK_OFFSET);
    int oldValue;
    do {
      oldValue = remSetLock.prepareInt();
    } while (oldValue != 0 || !remSetLock.attempt(oldValue, 1));
  }

  @Inline
  private static void unlock(Address region) {
    Address remSetLock = Region.metaDataOf(region, Region.METADATA_REMSET_LOCK_OFFSET);
    remSetLock.store(0);
  }

  /** Get PRT of remset of the `region` that contains `card` */
  @Inline
  @Uninterruptible
  private static Address preparePRT(Address region, Address card, boolean create) {
    // Get region index
    int regionIndex = region.diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_BLOCK).toInt();
    // Get card region index
    int cardRegionIndex = Region.of(card).diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_BLOCK).toInt();
    // Get PerRegionTable list, this is a page size
    Address prtList = rememberedSets.get(regionIndex);//rememberedSets[regionIndex];
    if (prtList.isZero()) { // create remset
      if (create) {
        // rememberedSets[regionIndex] = new int[TOTAL_REGIONS][];
        rememberedSets.set(regionIndex, Plan.metaDataSpace.acquire(REMSET_PAGES));
        prtList = rememberedSets.get(regionIndex); // rememberedSets[regionIndex];
      } else {
        return Address.zero();
      }
    }
    // Insert PerRegionTable if necessary
    Address prtEntry = prtList.plus(cardRegionIndex << Constants.LOG_BYTES_IN_ADDRESS);
    if (create && prtEntry.loadAddress().isZero()) {
      // prtList[cardRegionIndex] = new int[PER_REGION_TABLE_BYTES];
      prtEntry.store(Plan.metaDataSpace.acquire(PAGES_IN_PRT));
    }
    // Get PerRegionTable
    return prtEntry.loadAddress();
  }

  @Inline
  public static void addCard(Address region, Address card) {
    addCard(region, card, true);
  }

  @Inline
  private static void addCard(Address region, Address card, boolean lock) {
    if (lock) lock(region);

    Address prt = preparePRT(region, card, true);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!prt.isZero());

    // Insert card into the target PerRegionTable
    int remSetSize = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
    if (PerRegionTable.insert(prt, card)) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(PerRegionTable.contains(prt, card));
      //Log.write("Insert card ", card);
      //Log.writeln(" -> ", region);
      // Increase REMSET_SIZE
      Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).store(remSetSize + 1);
    }

    /*
    Address remSet = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).loadAddress();
    int remSetSize = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
    int remSetEntries = (Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).loadInt() << Constants.LOG_BYTES_IN_PAGE) >> Constants.LOG_BYTES_IN_ADDRESS;

    if (remSet.isZero() || remSetSize >= remSetEntries) {
      expandRemSetForRegion(region);
      remSet = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).loadAddress();
      remSetSize = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
      remSetEntries = (Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).loadInt() << Constants.LOG_BYTES_IN_PAGE) >> Constants.LOG_BYTES_IN_ADDRESS;
    }

    if (CardHashSet.insert(remSet, remSetEntries, card)) {
      // Increase REMSET_SIZE
      Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).store(remSetSize + 1);
    }
    */
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(CardHashSet.contains(remSet, remSetEntries, card));

    if (lock) unlock(region);
  }

  /*
  @Inline
  private static void removeCard(Address region, Address card) {
    int[] prt = preparePRT(region, card, false);
    if (prt == null) return;

    // Insert card into the target PerRegionTable
    int remSetSize = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
    if (PerRegionTable.remove(prt, card)) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!PerRegionTable.contains(prt, card));
      // Decrease REMSET_SIZE
      Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).store(remSetSize - 1);
    }
  }
  */


  @Inline
  public static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  @Uninterruptible
  public static class Processor {
    RegionSpace regionSpace;
    TraceLocal redirectPointerTrace;

    public Processor(TraceLocal redirectPointerTrace, RegionSpace regionSpace) {
      this.redirectPointerTrace = redirectPointerTrace;
      this.regionSpace = regionSpace;
    }

    public TransitiveClosure redirectPointerTransitiveClosure = new TransitiveClosure() {
      @Uninterruptible
      public void processEdge(ObjectReference source, Address slot) {
        ObjectReference ref = slot.loadObjectReference();
        if (!ref.isNull() && Space.isMappedObject(ref) && Space.isInSpace(regionSpace.getDescriptor(), ref) && Region.relocationRequired(Region.of(ref))) {
          redirectPointerTrace.processRootEdge(slot, true);
        }
      }
    };

    LinearScan cardLinearScan = new LinearScan() {
      @Override @Uninterruptible public void scan(ObjectReference object) {
        if (!object.isNull()) {
          //VM.scanning.scanObject(redirectPointerTransitiveClosure, object);
          redirectPointerTrace.traceObject(object, true);
        }
      }
    };

    /** Scan all cards in remsets of collection regions */
    public void processRemSets(AddressArray relocationSet, boolean concurrent, RegionSpace regionSpace) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int regionsToVisit = relocationSet.length();

      for (int i = 0; i < regionsToVisit; i++) {
        int cursor = regionsToVisit * id + i;
        if (cursor >= relocationSet.length()) continue;
        Address region = relocationSet.get(cursor);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!region.isZero());
        // Iterate all its PRTs
        int regionIndex = region.diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_BLOCK).toInt();
        Address prtList = rememberedSets.get(regionIndex);
        if (prtList.isZero()) continue;
        for (int j = 0; j < TOTAL_REGIONS; j++) {
          Address prt = prtList.plus(j << Constants.LOG_BYTES_IN_ADDRESS).loadAddress();
          if (prt.isZero()) continue;
          Address currentRegion = VM.HEAP_START.plus(j << Region.LOG_BYTES_IN_BLOCK);
          // Iterate all entries in prt
          for (int k = 0; k < INTS_IN_PRT; k++) {
            if (prt.plus(k << Constants.LOG_BYTES_IN_INT).loadInt() == 0) continue;
            int cardIndexStart = (k << Constants.LOG_BITS_IN_INT);
            for (int cardIndex = cardIndexStart; cardIndex < (cardIndexStart + Constants.BITS_IN_INT); cardIndex++) {
              if (!PerRegionTable.getBit(prt, cardIndex)) continue;
              Address card = currentRegion.plus(cardIndex << Region.Card.LOG_BYTES_IN_CARD);
              // This `card` is in rem-set of `region`
              if (!Space.isMappedAddress(card)) continue;
              if (Space.isInSpace(regionSpace.getDescriptor(), card)) {
                if (VM.VERIFY_ASSERTIONS)
                  VM.assertions._assert(Region.of(card).NE(EmbeddedMetaData.getMetaDataBase(card)));
                if (Region.relocationRequired(Region.of(card)))
                  continue;
              }
              if (Space.isInSpace(Plan.VM_SPACE, card)) continue;
              //Log.writeln("Scan card ", card);
              //Region.Card.tag = "UP";
              Region.Card.linearScan(cardLinearScan, card, false);
              /*for (int l = 0; l < relocationSet.length(); l++) {
                Address otherRegion = relocationSet.get(l);
                if (otherRegion.NE(region)) {
                  removeCard(otherRegion, card);
                }
              }*/
            }
          }
        }
      }
    }
  }

  /** Remove cards in collection regions from remsets of other regions & Release remsets of collection regions */
  @Inline
  @Uninterruptible
  public static void cleanupRemSetRefsToRelocationSet(RegionSpace regionSpace, AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int regionsToVisit = ceilDiv(TOTAL_REGIONS, workers);

    for (int i = 0; i < regionsToVisit; i++) {
      int cursor = regionsToVisit * id + i;
      if (cursor >= TOTAL_REGIONS) break;
      Address visitedRegion = VM.HEAP_START.plus(cursor << Region.LOG_BYTES_IN_BLOCK);
      // If this is a relocation region, clear its rem-sets
      if (Space.isInSpace(regionSpace.getDescriptor(), visitedRegion) && Region.of(visitedRegion).NE(EmbeddedMetaData.getMetaDataBase(visitedRegion)) && Region.relocationRequired(visitedRegion)) {
        Address pages = rememberedSets.get(cursor);
        if (!pages.isZero()) {
          Plan.metaDataSpace.release(pages);
          rememberedSets.set(cursor, Address.zero());
        }
        continue;
      }
      // Else, clear all PRT corresponds to CSet
      Address prtList = rememberedSets.get(cursor);
      if (prtList.isZero()) continue;
      for (int j = 0; j < relocationSet.length(); j++) {
        Address cRegion = relocationSet.get(j);
        if (cRegion.isZero()) continue;
        int index = cRegion.diff(VM.HEAP_START).toInt() >> Region.LOG_BYTES_IN_BLOCK;
        Address prtEntry = prtList.plus(index << Constants.LOG_BYTES_IN_ADDRESS);
        if (!prtEntry.loadAddress().isZero()) {
          Plan.metaDataSpace.release(prtEntry.loadAddress());
          prtEntry.store(Address.zero());
        }
      }/*
      for (int j = 0; j < remSet.length; j++) {
        Address remSetRegion = VM.HEAP_START.plus(j << Region.LOG_BYTES_IN_BLOCK);
        if (Space.isInSpace(regionSpace.getDescriptor(), remSetRegion) && Region.of(remSetRegion).NE(EmbeddedMetaData.getMetaDataBase(remSetRegion)) && Region.relocationRequired(remSetRegion)) {
          //Log.write("Remove region ", remSetRegion);
          //Log.writeln(" from remset of region ", visitedRegion);
          remSet[j] = null;
        }
      }*/
    }
  }
}
