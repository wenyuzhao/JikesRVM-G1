package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.TraceLocal;
//import org.mmtk.plan.markcopy.remset.MarkCopy;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.plan.concurrent.pureg1.PureG1;
import org.mmtk.utility.Constants;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.Lock;
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

  protected static LinearScan blockLinearScan = new LinearScan() {
    @Override @Uninterruptible public void scan(ObjectReference object) {
      // Forward this object
      if (VM.VERIFY_ASSERTIONS) {
        if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
          Log.write("Object ", object);
          if (ForwardingWord.isForwarded(object)) {
            Log.writeln(" is forwarded to ", RegionSpace.getForwardingPointer(object));
          } else {
            Log.writeln(" is being forwarded");
          }
        }
        VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
      }
      if (RegionSpace.Header.isMarked(object)) {

        Word oldState = ForwardingWord.attemptToForward(object);
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(!ForwardingWord.stateIsForwardedOrBeingForwarded(oldState));
          VM.assertions._assert(ForwardingWord.isForwardedOrBeingForwarded(object));
        }
        ObjectReference newObj = ForwardingWord.forwardObject(object, Plan.ALLOC_DEFAULT);

        if (VM.VERIFY_ASSERTIONS) {
          Log.write("Object ", object);
          Log.writeln(" -> ", newObj);
          VM.assertions._assert(ForwardingWord.isForwarded(object));
          VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(newObj));
        }
      }
    }
  };

  protected static void evacuateBlock(Address block) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero());
    // Forward all objects
    Region.linearScan(blockLinearScan, block);
  }

  public static void evacuateBlocks(AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int blocksToRelocate = ceilDiv(relocationSet.length(), workers);

    for (int i = 0; i < blocksToRelocate; i++) {
      int cursor = blocksToRelocate * id + i;
      if (cursor >= relocationSet.length()) break;
      Address block = relocationSet.get(cursor);
      evacuateBlock(block);
    }
  }

  @Uninterruptible
  public static class Processor {
    TraceLocal redirectPointerTrace;
    Address currentCard;

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

    public ObjectReference updateObject(ObjectReference object) {
      if (object.isNull()) return object;
      if (Space.getSpaceForObject(object) instanceof RegionSpace) {
        if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
          if (!ForwardingWord.isForwarded(object)) VM.objectModel.dumpObject(object);
          if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ForwardingWord.isForwarded(object));
          ObjectReference newObj = RegionSpace.getForwardingPointer(object);
          if (VM.VERIFY_ASSERTIONS) {
              Log.write("Ref ", object);
              Log.writeln(" ~> ", newObj);
          }
          if (!currentCard.isZero())
            RemSet.addCard(Region.of(newObj), currentCard);
          return newObj;
        }
        return object;
      }
      return object;
    }

    public void processRemSets(AddressArray relocationSet, boolean concurrent, RegionSpace regionSpace) {
      //lock.acquire();
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
        //if (Space.isInSpace(markBlockSpace.getDescriptor(), c)) MarkBlock.Card.linearScan(clearDeadMarkLinearScan, c);

        for (int j = 0; j < relocationSet.length(); j++) {
          Address block = relocationSet.get(j);
          if (containsCard(block, c)) {
            Region.Card.linearScan(cardLinearScan, c);
            break;
          }
        }
      }
      //lock.release();
    }

    public void updatePointers(AddressArray relocationSet, boolean concurrent, RegionSpace regionSpace) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int totalCards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
      int cardsToProcess = ceilDiv(totalCards, workers);

      for (int i = 0; i < cardsToProcess; i++) {
        int index = cardsToProcess * id + i;
        if (index >= totalCards) break;
        Address c = VM.HEAP_START.plus(index << Region.Card.LOG_BYTES_IN_CARD);

        if (Space.isInSpace(regionSpace.getDescriptor(), c) && Region.relocationRequired(Region.of(c))) {
          continue;
        }
        if (!Space.isMappedAddress(c)) continue;
        if (Space.isInSpace(Plan.VM_SPACE, c)) continue;

        for (int j = 0; j < relocationSet.length(); j++) {
          Address block = relocationSet.get(j);
          if (containsCard(block, c)) {
            if (VM.VERIFY_ASSERTIONS) {
              Log.write("Linear scan card ", c);
              Log.write(", range ", Region.Card.getCardAnchor(c));
              Log.write(" ..< ", Region.Card.getCardLimit(c));
              Log.write(", offsets ", Region.Card.getByte(Region.Card.anchors, Region.Card.hash(c)));
              Log.write(" ..< ", Region.Card.getByte(Region.Card.limits, Region.Card.hash(c)));
              Log.write(" in space: ");
              Log.writeln(Space.getSpaceForAddress(c).getName());
            }
            currentCard = c;
            Region.Card.linearScan(cardLinearScan, c);
            currentCard = Address.zero();
            break;
          }
        }
      }

      if (id == 0) {
        //VM.finalizableProcessor.forwardReadyForFinalize(redirectPointerTrace);
        for (Address b = regionSpace.firstBlock(); !b.isZero(); b = regionSpace.nextBlock(b)) {
          if (!Region.relocationRequired(b)) {
            if (VM.VERIFY_ASSERTIONS) {
              Log.write("Update objects in block ", b);
              Log.writeln(" ~ ", Region.getCursor(b));
            }
            Region.linearScan(cardLinearScan, b);
            //MarkBlock.mark(b, true);
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

  private static int DESC = 0;

  protected static LinearScan assertNoPointersToCSetLinearScan = new LinearScan() {
    TransitiveClosure transitiveClosure = new TransitiveClosure() {
      @Override @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
        ObjectReference ref = slot.loadObjectReference();
        if (ref.isNull() || !Space.isMappedObject(ref) || !Space.isInSpace(DESC, ref)) return;
        //if (!PureG1.regionSpace.isLive(source)) return;

        Address block = Region.of(ref);
        if (block.EQ(EmbeddedMetaData.getMetaDataBase(VM.objectModel.objectStartRef(ref)))) return;
        if (Region.relocationRequired(block) || !Region.allocated(block)) {
          VM.objectModel.dumpObject(source);
          VM.objectModel.dumpObject(ref);
          Log.write("Invalid reference ", source);
          Log.write(PureG1.regionSpace.isLive(source) ? "(live)" : "(dead)");
          Log.write(".", slot);
          Log.write(" = ", ref);
          Log.writeln(PureG1.regionSpace.isLive(ref) ? "(live)" : "(dead)");
          Log.write("Card ", Region.Card.of(source));
          if (RemSet.containsCard(block, Region.Card.of(source))) {
            Log.writeln(" is in the remset of block ", block);
          } else {
            Log.writeln(" is not in the remset of block ", block);
          }
        }
        VM.assertions._assert(!(Region.relocationRequired(block) || !Region.allocated(block)));
      }
    };
    @Override @Uninterruptible public void scan(ObjectReference object) {
      VM.scanning.scanObject(transitiveClosure, object);
    }
  };

  public static void assertNoPointersToCSet(RegionSpace space, AddressArray cset) {
    DESC = space.getDescriptor();
    int totalCards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
    for (int i = 0; i < totalCards; i++) {
      Address c = VM.HEAP_START.plus(i << Region.Card.LOG_BYTES_IN_CARD);

      if (!Space.isMappedAddress(c)) continue;
      if (Space.isInSpace(Plan.VM_SPACE, c)) continue;

      if (Space.isInSpace(DESC, c) && Region.relocationRequired(Region.of(c))) {
        continue;
      }

      Region.Card.linearScan(assertNoPointersToCSetLinearScan, c);
    }
  }

  protected static LinearScan assertPointersToCSetAreAllInRSetLinearScan = new LinearScan() {
    TransitiveClosure transitiveClosure = new TransitiveClosure() {
      @Override @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
        ObjectReference ref = slot.loadObjectReference();
        if (ref.isNull() || !Space.isMappedObject(ref) || !Space.isInSpace(DESC, ref)) return;
        Address block = Region.of(ref);
        Address sourceBlock = Region.of(source);
        if (sourceBlock.EQ(block)) return;
        if (Region.relocationRequired(block)) {
          if (!RemSet.containsCard(block, Region.Card.of(source))) {
            Log.write("Invalid reference ", source);
            Log.write(PureG1.regionSpace.isLive(source) ? "(live)" : "(dead)");
            Log.write(".", slot);
            Log.write(" = ", ref);
            Log.writeln(PureG1.regionSpace.isLive(ref) ? "(live)" : "(dead)");
            Log.write("Card ", Region.Card.of(source));
            Log.writeln(" is not in the remset of block ", block);
          }
          VM.assertions._assert(RemSet.containsCard(block, Region.Card.of(source)));
        }
      }
    };
    @Override @Uninterruptible public void scan(ObjectReference object) {
      VM.scanning.scanObject(transitiveClosure, object);
    }
  };

  public static void assertPointersToCSetAreAllInRSet(RegionSpace space, AddressArray cset) {
    DESC = space.getDescriptor();
    int totalCards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >> Region.Card.LOG_BYTES_IN_CARD;
    for (int i = 0; i < totalCards; i++) {
      Address c = VM.HEAP_START.plus(i << Region.Card.LOG_BYTES_IN_CARD);

      if (!Space.isMappedAddress(c)) continue;
      if (Space.isInSpace(Plan.VM_SPACE, c)) continue;

      if (Space.isInSpace(DESC, c) && Region.relocationRequired(Region.of(c))) {
        continue;
      }

      Region.Card.linearScan(assertPointersToCSetAreAllInRSetLinearScan, c);
    }
  }

  public static void assertCorrectness(RegionSpace space) {
    Address b = space.firstBlock();
    while (!b.isZero()) {
      for (Address c = VM.HEAP_START; c.LT(VM.HEAP_END); c = c.plus(Region.Card.BYTES_IN_CARD)) {
        if (!Space.isMappedAddress(c)) continue;

        if (containsCard(b, c)) {
          // Should be a card within the heap
          VM.assertions._assert(Space.isMappedAddress(c));
          // Cannot contains a relocated card
          if (space.isInSpace(space.descriptor, c)) {
            VM.assertions._assert(!Region.relocationRequired(Region.of(c)));
          }
        }
      }
      b = space.nextBlock(b);
    }
  }
}
