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
  private static final int PAGES_IN_REMSET;

  static {
    int cards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
    PAGES_IN_REMSET = ceilDiv(cards, Constants.BYTES_IN_PAGE << Constants.LOG_BITS_IN_BYTE);
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
  private static int hash(Address card) {
    return card.diff(VM.HEAP_START).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
  }

  @Inline
  public static void addCard(Address region, Address card) {
    lock(region);
    Address remSet = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).loadAddress();
    if (remSet.isZero()) {
      remSet = space.acquire(PAGES_IN_REMSET);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!remSet.isZero());
      Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).store(remSet);
    }

    if (!containsCard(region, card)) {
      // Increase REMSET_SIZE
      int remSetSize = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
      Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).store(remSetSize + 1);
      // Set bit
      int index = hash(card);
      int byteIndex = index >> 3;
      int bitIndex = index ^ (byteIndex << 3);
      Address addr = remSet.plus(byteIndex);
      byte b = addr.loadByte();
      addr.store((byte) (b | (1 << (7 - bitIndex))));
    }

    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(containsCard(region, card));

    unlock(region);
  }

  @Inline
  private static void removeCard(Address region, Address card) {
    lock(region);

    Address remSet = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).loadAddress();
    if (remSet.isZero()) return;

    if (containsCard(region, card)) {
      // Decrease REMSET_SIZE
      int remSetSize = Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
      Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).store(remSetSize - 1);
      // Set bit
      int index = hash(card);
      int byteIndex = index >> 3;
      int bitIndex = index ^ (byteIndex << 3);
      Address addr = remSet.plus(byteIndex);
      byte b = addr.loadByte();
      addr.store((byte) (b & ~(1 << (7 - bitIndex))));
    }

    unlock(region);
  }

  @Inline
  public static boolean containsCard(Address region, Address card) {
    Address remSet = Region.metaDataOf(region, Region.METADATA_REMSET_POINTER_OFFSET).loadAddress();
    if (remSet.isZero()) return false;
    int index = hash(card);
    int byteIndex = index >> 3;
    int bitIndex = index ^ (byteIndex << 3);
    Address addr = remSet.plus(byteIndex);
    byte b = addr.loadByte();
    return ((byte) (b & (1 << (7 - bitIndex)))) != ((byte) 0);
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
        if (!object.isNull()) {
            VM.scanning.scanObject(redirectPointerTrace, object);
        }
      }
    };

    public void processRemSets(AddressArray relocationSet, boolean concurrent, RegionSpace regionSpace) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int totalCards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
      int cardsToProcess = ceilDiv(totalCards, workers);

      for (int i = 0; i < cardsToProcess; i++) {
        int index = cardsToProcess * id + i;
        if (index >= totalCards) break;
        Address c = VM.HEAP_START.plus(index << Region.Card.LOG_BYTES_IN_CARD);

        if (!Space.isMappedAddress(c)) continue;
        if (Space.isInSpace(regionSpace.getDescriptor(), c) && (Region.of(c).EQ(EmbeddedMetaData.getMetaDataBase(c)) || Region.relocationRequired(Region.of(c)))) {
          continue;
        }
        if (Space.isInSpace(Plan.VM_SPACE, c)) continue;

        for (int j = 0; j < relocationSet.length(); j++) {
          Address block = relocationSet.get(j);
          if (containsCard(block, c)) {
            Region.Card.linearScan(cardLinearScan, c);
            break;
          }
        }
      }
    }
  }

  @Inline
  public static void clearRemsetMetaForBlock(Address targetBlock) {
    Address targetBlockLimit = targetBlock.plus(Region.BYTES_IN_BLOCK);
    RegionSpace space =  (RegionSpace) Space.getSpaceForAddress(targetBlock);
    Address block = space.firstBlock();
    while (!block.isZero()) {
      if (!Region.relocationRequired(block)) {
        for (Address c = targetBlock; c.LT(targetBlockLimit); c = c.plus(Region.Card.BYTES_IN_CARD)) {
          removeCard(block, c);
        }
      }
      block = space.nextBlock(block);
    }
  }

  public static void clearRemsetForRelocationSet(AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int blocksToRelease = ceilDiv(relocationSet.length(), workers);

    for (int i = 0; i < blocksToRelease; i++) {
      int cursor = blocksToRelease * id + i;
      if (cursor >= relocationSet.length()) break;
      Address block = relocationSet.get(cursor);
      if (!block.isZero()) {
        clearRemsetMetaForBlock(block);
        Address remSetPointer = Region.metaDataOf(block, Region.METADATA_REMSET_POINTER_OFFSET);
        Address remSet = remSetPointer.loadAddress();
        if (!remSet.isZero()) {
          lock(block);
          remSetPointer.store(Address.zero());
          space.release(remSet);
          unlock(block);
        }
      }
    }
  }
}
