package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.TraceLocal;
import org.mmtk.utility.Constants;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

@Uninterruptible
public class RemSet {
  private static Space space = Plan.metaDataSpace;

  @Uninterruptible
  private static class CardHashSet {
    @Inline
    private static int hash(Address card) {
      return card.toWord().rshl(Region.Card.LOG_BYTES_IN_CARD).toInt();
    }
    @Inline
    private static boolean insert(Address table, int entries, Address card) {
      Address cursor = table.plus((hash(card) % entries) << Constants.LOG_BYTES_IN_ADDRESS);
      Address limit = table.plus(entries << Constants.LOG_BYTES_IN_ADDRESS);
      for (Address i = cursor; i.LT(limit); i = i.plus(Constants.BYTES_IN_ADDRESS)) {
        Address c = i.loadAddress();
        if (c.EQ(card)) return false;
        if (c.isZero()) {
          i.store(card);
          return true;
        }
      }
      for (Address i = table; i.LT(cursor); i = i.plus(Constants.BYTES_IN_ADDRESS)) {
        Address c = i.loadAddress();
        if (c.EQ(card)) return false;
        if (c.isZero()) {
          i.store(card);
          return true;
        }
      }
      VM.assertions.fail("HashSet Overflow");
      return false;
    }
    @Inline
    private static boolean contains(Address table, int entries, Address card) {
      Address cursor = table.plus((hash(card) % entries) << Constants.LOG_BYTES_IN_ADDRESS);
      Address limit = table.plus(entries << Constants.LOG_BYTES_IN_ADDRESS);
      for (Address i = cursor; i.LT(limit); i = i.plus(Constants.BYTES_IN_ADDRESS)) {
        if (i.loadAddress().EQ(card)) return true;
      }
      for (Address i = table; i.LT(cursor); i = i.plus(Constants.BYTES_IN_ADDRESS)) {
        if (i.loadAddress().EQ(card)) return true;
      }
      return false;
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

  @Inline
  private static void expandRemSetForRegion(Address region) {
    Address oldRemSet = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).loadAddress();
    int oldRemSetPages = Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).loadInt();
    int oldRemSetBytes = oldRemSetPages << Constants.LOG_BYTES_IN_PAGE;
    int newRemSetPages = oldRemSet.isZero() ? 1 : (oldRemSetPages << 1);

    Address newRemSet = space.acquire(newRemSetPages);
    Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).store(newRemSet);
    Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).store(newRemSetPages);
    // Copy
    Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).store(0);
    for (int i = 0; i < oldRemSetBytes; i += Constants.BYTES_IN_ADDRESS) {
      Address card = oldRemSet.plus(i).loadAddress();
      if (!card.isZero()) addCard(region, card, false);
    }

    if (!oldRemSet.isZero()) space.release(oldRemSet);
  }

  @Inline
  public static void addCard(Address region, Address card) {
    addCard(region, card, true);
  }

  @Inline
  private static void addCard(Address region, Address card, boolean lock) {
    if (lock) lock(region);

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

    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(CardHashSet.contains(remSet, remSetEntries, card));

    if (lock) unlock(region);
  }

  @Inline
  protected static int ceilDiv(int a, int b) {
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
        if (!object.isNull()) VM.scanning.scanObject(redirectPointerTrace, object);
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
      }
    }
  }

  @Inline
  private static void cleanupRemSetRefsToRelocationSetForRegion(RegionSpace regionSpace, Address region, AddressArray relocationSet) {
    lock(region);
    Address remSet = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).loadAddress();
    Address limit = remSet.plus(Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).loadInt() << Constants.LOG_BYTES_IN_PAGE);
    for (Address i = remSet; i.LT(limit); i = i.plus(Constants.BYTES_IN_ADDRESS)) {
      Address c = i.loadAddress();
      if (c.isZero() || !Space.isInSpace(regionSpace.getDescriptor(), c)) continue;
      Address cardRegion = Region.of(c);
      for (int j = 0; j < relocationSet.length(); j++) {
        if (relocationSet.get(j).EQ(cardRegion)) {
          // This card is in CSet, remove this card from RemSet.
          i.store(Address.zero());
          int oldRemSetSize = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
          if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldRemSetSize > 0);
          Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).store(oldRemSetSize - 1);
          break;
        }
      }
    }
    unlock(region);
  }

  /** Remove cards in collection regions from remsets of other regions & Release remsets of collection regions */
  @Inline
  public static void cleanupRemSetRefsToRelocationSet(RegionSpace regionSpace, AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
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
    }
  }

  @Inline
  public static void releaseRemSetsOfRelocationSet(AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;

    if (id == 0) {
      Address region = relocationSet.get(0);
      Address remSetPointer = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET);
      Address remSet = remSetPointer.loadAddress();
      if (!remSet.isZero()) {
        lock(region);
        remSetPointer.store(Address.zero());
        space.release(remSet);
        unlock(region);
      }
    }
  }
}
