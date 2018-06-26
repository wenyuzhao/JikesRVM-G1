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
import org.vmmagic.pragma.UninterruptibleNoWarn;
import org.vmmagic.unboxed.*;

@Uninterruptible
public class RemSet {
  private static Space space = Plan.metaDataSpace;
  private static int[][][] rememberedSets;
  private final static int PER_REGION_TABLE_BYTES;
  private final static int TOTAL_REGIONS;

  static {
    Word heapSize = VM.HEAP_END.diff(VM.HEAP_START).toWord();
    TOTAL_REGIONS = heapSize.rshl(Region.LOG_BYTES_IN_BLOCK).toInt();
    rememberedSets = new int[TOTAL_REGIONS][][];
    PER_REGION_TABLE_BYTES = (Region.BYTES_IN_BLOCK >> Region.Card.LOG_BYTES_IN_CARD) >> Constants.LOG_BITS_IN_BYTE;
  }

  @Uninterruptible
  private static class PerRegionTable {
    @Inline
    private static boolean attemptBitInBuffer(int[] buf, int index, boolean newBit) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldBit != newBit);
      int intIndex = index >> Constants.LOG_BITS_IN_INT;
      int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(intIndex >= 0 && intIndex < buf.length);
        VM.assertions._assert(bitIndex >= 0 && bitIndex < Constants.BITS_IN_INT);
      }
      Offset offset = Offset.fromIntZeroExtend(intIndex << Constants.LOG_BYTES_IN_INT);
      int oldValue, newValue;
      do {
        // Get old int
        oldValue = buf[intIndex];
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
    static boolean getBit(int[] buf, int index) {
      int intIndex = index >> Constants.LOG_BITS_IN_INT;
      int bitIndex = index ^ (intIndex << Constants.LOG_BITS_IN_INT);
      int entry = buf[intIndex];
      return (entry & (1 << (31 - bitIndex))) != 0;
    }

    static boolean contains(int[] prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
      return getBit(prt, index);
    }

    static boolean insert(int[] prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
      //Address ptr = ObjectReference.fromObject(prt).toAddress().plus(index << Constants.LOG_BYTES_IN_INT);
      return attemptBitInBuffer(prt, index, true);
    }

    static boolean remove(int[] prt, Address card) {
      int index = card.diff(Region.of(card)).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
      return attemptBitInBuffer(prt, index, false);
    }
  }

  @Inline
  private static void lock(Address region) {
    /*Address remSetLock = Region.metaDataOf(region, Region.METADATA_REMSET_LOCK_OFFSET);
    int oldValue;
    do {
      oldValue = remSetLock.prepareInt();
    } while (oldValue != 0 || !remSetLock.attempt(oldValue, 1));*/
  }

  @Inline
  private static void unlock(Address region) {
    //Address remSetLock = Region.metaDataOf(region, Region.METADATA_REMSET_LOCK_OFFSET);
    //remSetLock.store(0);
  }

  @Inline
  @UninterruptibleNoWarn
  private static int[] preparePRT(Address region, Address card, boolean create) {
    // Get region index
    int regionIndex = region.diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_BLOCK).toInt();
    // Get card region index
    int cardRegionIndex = Region.of(card).diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_BLOCK).toInt();
    // Get PerRegionTable list
    int[][] prtList = rememberedSets[regionIndex];
    if (prtList == null) {
      if (create) {
        rememberedSets[regionIndex] = new int[TOTAL_REGIONS][];
        prtList = rememberedSets[regionIndex];
      } else {
        return null;
      }
    }
    // Insert PerRegionTable if necessary
    if (create && prtList[cardRegionIndex] == null)
      prtList[cardRegionIndex] = new int[PER_REGION_TABLE_BYTES];
    // Get PerRegionTable
    return prtList[cardRegionIndex];
  }

  @Inline
  public static void addCard(Address region, Address card) {
    addCard(region, card, true);
  }

  @Inline
  private static void addCard(Address region, Address card, boolean lock) {
    if (lock) lock(region);

    int[] prt = preparePRT(region, card, true);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(prt != null);

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


  @Inline
  public static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  @Uninterruptible
  public static class Processor {
    TraceLocal redirectPointerTrace;

    public Processor(TraceLocal redirectPointerTrace) {
      this.redirectPointerTrace = redirectPointerTrace;
    }

    LinearScan cardLinearScan = new LinearScan() {
      @Override @Uninterruptible public void scan(ObjectReference object) {
        if (!object.isNull()) {
          //VM.scanning.scanObject(redirectPointerTrace, object);
          redirectPointerTrace.traceObject(object, true);
        }
      }
    };

    public void unionRemSets(RegionSpace regionSpace, AddressArray relocationSet, boolean concurrent) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int regionsToVisit = ceilDiv(relocationSet.length(), workers);

      Address targetRegion = relocationSet.get(0);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!targetRegion.isZero());

      for (int i = 0; i < regionsToVisit; i++) {
        int cursor = regionsToVisit * id + i;
        if (cursor == 0) continue;
        if (cursor >= relocationSet.length()) break;

        Address region = relocationSet.get(cursor);
        if (!region.isZero()) {
          // Add all cards remembered by this region to targetRegion
          Address remSetPointer = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET);
          Address remSet = remSetPointer.loadAddress();
          if (!remSet.isZero()) {
            Address limit = remSet.plus(Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).loadInt() << Constants.LOG_BYTES_IN_PAGE);
            for (Address j = remSet; j.LT(limit); j = j.plus(Constants.BYTES_IN_ADDRESS)) {
              Address c = j.loadAddress();
              if (c.isZero() || (Space.isInSpace(regionSpace.getDescriptor(), c) && Region.relocationRequired(Region.of(c))))
                continue;
              addCard(targetRegion, c);
            }
            // Release current remSet
            lock(region);
            remSetPointer.store(Address.zero());
            space.release(remSet);
            unlock(region);
          }
        }
      }
    }

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
        int[][] prtList = rememberedSets[regionIndex];
        if (prtList == null) continue;
        for (int j = 0; j < prtList.length; j++) {
          int[] prt = prtList[j];
          if (prt == null) continue;
          Address currentRegion = VM.HEAP_START.plus(j << Region.LOG_BYTES_IN_BLOCK);
          // Iterate all entries in prt
          for (int k = 0; k < prt.length; k++) {
            if (prt[k] == 0) continue;
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
              Region.Card.tag = "UP";
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


      /*Word heapSize = VM.HEAP_END.diff(VM.HEAP_START).toWord();
      int totalCards = heapSize.rshl(Region.Card.LOG_BYTES_IN_CARD).toInt();
      int cardsToVisit = ceilDiv(totalCards, workers);

      for (int i = 0; i < cardsToVisit; i++) {
        int cursor = cardsToVisit * id + i;
        if (cursor >= totalCards) continue;
        Address card = VM.HEAP_START.plus(cursor << Region.Card.LOG_BYTES_IN_CARD);

        if (!Space.isMappedAddress(card)) continue;
        if (Space.isInSpace(regionSpace.getDescriptor(), card) && (Region.of(card).EQ(EmbeddedMetaData.getMetaDataBase(card)) || Region.relocationRequired(Region.of(card)))) {
          continue;
        }
        if (Space.isInSpace(Plan.VM_SPACE, card)) continue;

        for (int j = 0; j < relocationSet.length(); j++) {
          Address region = relocationSet.get(j);
          if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!region.isZero());
          int[] prt = preparePRT(region, card, false);
          if (prt != null && PerRegionTable.contains(prt, card)) {
            Region.Card.linearScan(cardLinearScan, card);
            break;
          }
        }
      }*/

      /*
      Address remSet = Region.metaDataOf(relocationSet.get(0), Region.METADATA_REMSET_POINTER_OFFSET).loadAddress();
      int remSetEntries = (Region.metaDataOf(relocationSet.get(0), Region.METADATA_REMSET_PAGES_OFFSET).loadInt() << Constants.LOG_BYTES_IN_PAGE) >> Constants.LOG_BYTES_IN_ADDRESS;
      int entriesToVisit = ceilDiv(remSetEntries, workers);

      for (int i = 0; i < entriesToVisit; i++) {
        int cursor = entriesToVisit * id + i;
        if (cursor >= remSetEntries) break;
        Address c = remSet.plus(cursor << Constants.LOG_BYTES_IN_ADDRESS).loadAddress();
        if (c.isZero()) continue;
        if (!Space.isMappedAddress(c)) continue;
        if (Space.isInSpace(regionSpace.getDescriptor(), c) && (Region.of(c).EQ(EmbeddedMetaData.getMetaDataBase(c)) || Region.relocationRequired(Region.of(c)))) {
          continue;
        }
        if (Space.isInSpace(Plan.VM_SPACE, c)) continue;
        Region.Card.linearScan(cardLinearScan, c);
      }*/
    }

    /*RegionSpace _regionSpace;

    TransitiveClosure verificationTC = new TransitiveClosure() {
      @Override @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
        ObjectReference ref = slot.loadObjectReference();
        if (ref.isNull()) return;
        if (!Space.isInSpace(_regionSpace.getDescriptor(), ref)) {
          if (!Space.isMappedObject(ref)) {
            Log.write(source);
            Log.write(".", slot);
            Log.write(": ", ref);
            Log.writeln(" outside of heap ");
            VM.objectModel.dumpObject(ref);
            VM.assertions._assert(false);
          }
          return;
        }
        if (!Space.isInSpace(_regionSpace.getDescriptor(), ref)
        if ()
        VM.assertions._assert();
      }
    };

    LinearScan verificationLinearScan = new LinearScan() {
      @Override @Uninterruptible public void scan(ObjectReference object) {
        VM.assertions._assert(!object.isNull());
        VM.scanning.scanObject(verificationTC, object);
      }
    };

    public void assertAllRefsUpdated(RegionSpace regionSpace) {
      _regionSpace = regionSpace;
      Address block = regionSpace.firstBlock();
      while (!block.isZero()) {
        Region.linearScan(verificationLinearScan, block);
        block = regionSpace.nextBlock(block);
      }
    }*/
  }

  /** Remove cards in collection regions from remsets of other regions & Release remsets of collection regions */
  @Inline
  @UninterruptibleNoWarn
  public static void cleanupRemSetRefsToRelocationSet(RegionSpace regionSpace, AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int regionsToVisit = ceilDiv(rememberedSets.length, workers);

    for (int i = 0; i < regionsToVisit; i++) {
      int cursor = regionsToVisit * id + i;
      if (cursor >= rememberedSets.length) break;
      Address visitedRegion = VM.HEAP_START.plus(cursor << Region.LOG_BYTES_IN_BLOCK);
      // If this is a relocation region, clear its rem-sets
      if (Space.isInSpace(regionSpace.getDescriptor(), visitedRegion) && Region.of(visitedRegion).NE(EmbeddedMetaData.getMetaDataBase(visitedRegion)) && Region.relocationRequired(visitedRegion)) {
        rememberedSets[cursor] = null;
        continue;
      }
      // Else, clear all PRT corresponds to CSet
      int[][] remSet = rememberedSets[cursor];
      if (remSet == null) continue;
      for (int j = 0; j < remSet.length; j++) {
        Address remSetRegion = VM.HEAP_START.plus(j << Region.LOG_BYTES_IN_BLOCK);
        if (Space.isInSpace(regionSpace.getDescriptor(), remSetRegion) && Region.of(remSetRegion).NE(EmbeddedMetaData.getMetaDataBase(remSetRegion)) && Region.relocationRequired(remSetRegion)) {
          //Log.write("Remove region ", remSetRegion);
          //Log.writeln(" from remset of region ", visitedRegion);
          remSet[j] = null;
        }
      }
    }

    /*int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    AddressArray regions = regionSpace.allocatedRegions();
    int regionsToVisit = ceilDiv(regions.length(), workers);

    for (int i = 0; i < regionsToVisit; i++) {
      int cursor = regionsToVisit * id + i;
      if (cursor >= regions.length()) break;
      Address region = regions.get(cursor);
      if (!region.isZero() && !Region.relocationRequired(region)) {
        cleanupRemSetRefsToRelocationSetForRegion(regionSpace, region, relocationSet);
      }
    }*/
  }

  @Inline
  @UninterruptibleNoWarn
  public static void releaseRemSetsOfRelocationSet(AddressArray relocationSet, boolean concurrent) {
    /*int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int regionsToVisit = ceilDiv(relocationSet.length(), workers);

    for (int i = 0; i < regionsToVisit; i++) {
      int cursor = regionsToVisit * id + i;
      if (cursor >= relocationSet.length()) break;
      Address region = relocationSet.get(cursor);
      // Get region index
      int regionIndex = region.diff(VM.HEAP_START).toWord().rshl(Region.LOG_BYTES_IN_BLOCK).toInt();
      rememberedSets[regionIndex] = null;
    }*/
    /*if (id == 0) {
      Address region = relocationSet.get(0);
      Address remSetPointer = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET);
      Address remSet = remSetPointer.loadAddress();
      if (!remSet.isZero()) {
        lock(region);
        remSetPointer.store(Address.zero());
        space.release(remSet);
        unlock(region);
      }
    }*/
  }


}
