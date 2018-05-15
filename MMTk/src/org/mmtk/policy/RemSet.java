package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.plan.markcopy.remset.MarkCopy;
import org.mmtk.policy.immix.Block;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Conversions;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BlockAllocator;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.pragma.UninterruptibleNoWarn;
import org.vmmagic.unboxed.*;

@Uninterruptible
public class RemSet {

  /** A bitmap of all cards in a block */
  /*@Uninterruptible
  private static class CardHashTable {
    private static final Offset KEY_OFFSET = Offset.zero();
    private static final Offset DATA_OFFSET = Offset.fromIntSignExtend(Constants.BYTES_IN_WORD);
    private static final Extent entrySize = Extent.fromIntZeroExtend(Constants.BYTES_IN_ADDRESS);
    private static int LOG_ENTRIES_PER_REMSET;

    @Inline
    private static int computeCardHash(Address card) {
      return card.toWord().rshl(9).toInt() % MarkBlock.ADDITIONAL_METADATA.toInt();
    }

    @Inline
    private static Address getEntry(Address hashtbl, int index) {
      return hashtbl.plus(Extent.fromIntZeroExtend(index * entrySize.toInt()));
    }

    @Inline
    public static final Address getEntry(Address hashtbl, Address card, boolean create) {
      Word mask = Word.fromIntZeroExtend((1 << LOG_ENTRIES_PER_REMSET) - 1);
      int startIndex = computeCardHash(card);
      int index = startIndex;
      Address curAddress;
      Address entry;
      do {
        entry = getEntry(hashtbl, index);
        curAddress = entry.loadAddress(KEY_OFFSET);
        index = (index + 1) & mask.toInt();
      } while(curAddress.NE(card) &&
        !curAddress.isZero() &&
        index != startIndex);

      if (index == startIndex) {
        VM.assertions.fail("No room left in table!");
      }

      if (curAddress.isZero()) {
        if (!create) return Address.zero();
        entry.store(card, KEY_OFFSET);
      }

      return entry;
    }
  }*/

  /* RemSet of a block */
  @Inline
  public static Address of(Address block) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.isAligned(block));
    return MarkBlock.additionalMetadataStart(block).plus(REMSET_OFFSET);
  }

  @Inline
  private static int hash(Address card) {
    return card.diff(VM.HEAP_START).toInt() >> MarkBlock.Card.LOG_BYTES_IN_CARD;
  }

  static Lock lock2 = VM.newLock("18436rtfgyvc0218");

  @Inline
  public static void addCard(Address block, Address card) {
    lock2.acquire();
    //Log.write("Add card ", card);
    //Log.writeln(" -> ", block);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(MarkBlock.isAligned(block));
      VM.assertions._assert(MarkBlock.Card.isAligned(card));
      int memorySize = VM.HEAP_END.diff(VM.HEAP_START).toInt();
      int totalCards = memorySize >> MarkBlock.Card.LOG_BYTES_IN_CARD;
      /*Log.writeln("HEAP_START=", VM.HEAP_START);
      Log.writeln("HEAP_END=", VM.HEAP_END);
      Log.writeln("totalCards=", totalCards);
      Log.writeln("REMSET_EXTENT=", REMSET_EXTENT);*/
      VM.assertions._assert(REMSET_EXTENT.GE(Extent.fromIntZeroExtend(totalCards / 8)));
    }
    Address cardTable = RemSet.of(block);
    int index = hash(card);
    int byteIndex = index >> 3;
    int bitIndex = index ^ (byteIndex << 3);
    Address addr = cardTable.plus(byteIndex);
    byte b = addr.loadByte();
    addr.store((byte) (b | (1 << (7 - bitIndex))));
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(containsCard(block, card));
    lock2.release();
  }

  @Inline
  public static void removeCard(Address block, Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(MarkBlock.isAligned(block));
      VM.assertions._assert(MarkBlock.Card.isAligned(card));
    }
    Address cardTable = RemSet.of(block);
    int index = hash(card);
    int byteIndex = index >> 3;
    int bitIndex = index ^ (byteIndex << 3);
    Address addr = cardTable.plus(byteIndex);
    byte b = addr.loadByte();
    addr.store((byte) (b & ~(1 << (7 - bitIndex))));
  }

  @Inline
  public static boolean containsCard(Address block, Address card) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(MarkBlock.isAligned(block));
      VM.assertions._assert(MarkBlock.Card.isAligned(card));
    }
    Address cardTable = RemSet.of(block);
    int index = hash(card);
    int byteIndex = index >> 3;
    int bitIndex = index ^ (byteIndex << 3);
    Address addr = cardTable.plus(byteIndex);
    byte b = addr.loadByte();
    return ((byte) (b & (1 << (7 - bitIndex)))) != ((byte) 0);
  }

  @Inline
  protected static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  public static final Offset REMSET_OFFSET;
  public static final Extent REMSET_EXTENT;

  static {
    // Preserve as much meta pages as possible
    MarkBlock.setAdditionalMetadataPagesPerRegion(MarkBlock.METADATA_PAGES_PER_REGION - MarkBlock.USED_METADATA_PAGES_PER_REGION);
    REMSET_OFFSET = Offset.fromIntZeroExtend(0);
    REMSET_EXTENT = Extent.fromIntZeroExtend(MarkBlock.ADDITIONAL_METADATA.toInt() - REMSET_OFFSET.toInt());
  }

  protected static LinearScan blockLinearScan = new LinearScan() {
    @Override @Uninterruptible public void scan(ObjectReference object) {
      // Forward this object
      if (VM.VERIFY_ASSERTIONS) {
        if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
          Log.write("Object ", object);
          if (ForwardingWord.isForwarded(object)) {
            Log.writeln(" is forwarded to ", MarkBlockSpace.getForwardingPointer(object));
          } else {
            Log.writeln(" is being forwarded");
          }
        }
        VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
      }
      if (MarkBlockSpace.Header.isMarked(object)) {

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
    MarkBlock.linearScan(blockLinearScan, block);
  }

  public static void evacuateBlocks(AddressArray relocationSet, boolean concurrent) {
    if (VM.activePlan.collector().getId() == 0) {
      for (int i = 0; i < relocationSet.length(); i++) {
        Address block = relocationSet.get(i);
        Log.writeln("Evacuate block ", block);
        evacuateBlock(block);
      }
    }
    /*
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int blocksToRelocate = ceilDiv(relocationSet.length(), workers);

    Log.write("Collector ", VM.activePlan.collector().getId());
    Log.write(" workers ", workers);
    Log.write(" relocationSet.length() ", relocationSet.length());
    Log.writeln(" => ", blocksToRelocate);

    for (int i = 0; i < blocksToRelocate; i++) {
      int cursor = blocksToRelocate * id + i;
      if (cursor >= relocationSet.length()) break;
      Address block = relocationSet.get(cursor);
      evacuateBlock(block);
    }
    */
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
          if (Space.isInSpace(MarkCopy.MC, object) && !MarkBlockSpace.Header.isMarked(object)) return;
          VM.scanning.scanObject(redirectPointerTrace, object);
        }
      }
    };

    public ObjectReference updateObject(ObjectReference object) {
      if (object.isNull()) return object;
      if (Space.isInSpace(MarkCopy.MC, object)) {
        if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
          if (!ForwardingWord.isForwarded(object)) VM.objectModel.dumpObject(object);
          if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ForwardingWord.isForwarded(object));
          ObjectReference newObj = MarkBlockSpace.getForwardingPointer(object);
          if (VM.VERIFY_ASSERTIONS) {
              Log.write("Ref ", object);
              Log.writeln(" ~> ", newObj);
          }
          if (!currentCard.isZero())
            RemSet.addCard(MarkBlock.of(newObj.toAddress()), currentCard);
          return newObj;
        }
        return object;
      }
      return object;
    }

    public void updatePointers(AddressArray relocationSet, boolean concurrent) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int totalCards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >> MarkBlock.Card.LOG_BYTES_IN_CARD;
      int cardsToProcess = ceilDiv(totalCards, workers);

      for (int i = 0; i < cardsToProcess; i++) {
        int index = cardsToProcess * id + i;
        if (index >= totalCards) break;
        Address c = VM.HEAP_START.plus(index << MarkBlock.Card.LOG_BYTES_IN_CARD);

        if (Space.isInSpace(MarkCopy.MC, c) && MarkBlock.relocationRequired(MarkBlock.of(c))) {
          continue;
        }
        if (!Space.isMappedAddress(c)) continue;
        if (Space.isInSpace(Plan.VM_SPACE, c)) continue;

        for (int j = 0; j < relocationSet.length(); j++) {
          Address block = relocationSet.get(j);
          if (containsCard(block, c)) {
            if (VM.VERIFY_ASSERTIONS) {
              Log.write("Linear scan card ", c);
              Log.write(", range ", MarkBlock.Card.getCardAnchor(c));
              Log.write(" ..< ", MarkBlock.Card.getCardLimit(c));
              Log.write(", offsets ", MarkBlock.Card.getByte(MarkBlock.Card.anchors, MarkBlock.Card.hash(c)));
              Log.write(" ..< ", MarkBlock.Card.getByte(MarkBlock.Card.limits, MarkBlock.Card.hash(c)));
              Log.write(" in space: ");
              Log.writeln(Space.getSpaceForAddress(c).getName());
            }
            currentCard = c;
            MarkBlock.Card.linearScan(cardLinearScan, c);
            currentCard = Address.zero();
            break;
          }
        }
      }

      if (id == 0) {
        //VM.finalizableProcessor.forwardReadyForFinalize(redirectPointerTrace);
        for (Address b = MarkCopy.markBlockSpace.firstBlock(); !b.isZero(); b = MarkCopy.markBlockSpace.nextBlock(b)) {
          if (!MarkBlock.relocationRequired(b)) {
            if (VM.VERIFY_ASSERTIONS) {
              Log.write("Update objects in block ", b);
              Log.writeln(" ~ ", MarkBlock.getCursor(b));
            }
            MarkBlock.linearScan(cardLinearScan, b);
            MarkBlock.mark(b, true);
          }
        }
      }
    }
  }

  @Inline
  public static void clearRemsetMedaForBlock(Address targetBlock) {
    Address targetBlockLimit = targetBlock.plus(MarkBlock.BYTES_IN_BLOCK);
    MarkBlockSpace space =  (MarkBlockSpace) Space.getSpaceForAddress(targetBlock);
    Address block = space.firstBlock();
    while (!block.isZero()) {
      for (Address c = targetBlock; c.LT(targetBlockLimit); c = c.plus(MarkBlock.Card.BYTES_IN_CARD)) {
        if (containsCard(block, c)) {
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
        clearRemsetMedaForBlock(block);
      }
    }
  }

  public static void assertCorrectness(MarkBlockSpace space) {
    Address b = space.firstBlock();
    while (!b.isZero()) {
      for (Address c = VM.HEAP_START; c.LT(VM.HEAP_END); c = c.plus(MarkBlock.Card.BYTES_IN_CARD)) {
        if (!Space.isMappedAddress(c)) continue;

        if (containsCard(b, c)) {
          // Should be a card within the heap
          VM.assertions._assert(Space.isMappedAddress(c));
          // Cannot contains a relocated card
          if (space.isInSpace(space.descriptor, c)) {
            VM.assertions._assert(!MarkBlock.relocationRequired(MarkBlock.of(c)));
          }
        }
      }
      b = space.nextBlock(b);
    }
  }
}
