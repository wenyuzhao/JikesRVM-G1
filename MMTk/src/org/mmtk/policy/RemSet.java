package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.plan.markcopy.remset.MarkCopy;
import org.mmtk.utility.Constants;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
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
  @Uninterruptible
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
  }

  /* RemSet of a block */
  public static Address of(Address block) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.isAligned(block));
    return MarkBlock.additionalMetadataStart(block).plus(REMSET_OFFSET);
  }



  @Inline
  public static void addCard(Address block, Address card) {
    Address hashtbl = RemSet.of(block);
    CardHashTable.getEntry(hashtbl, card, true);
  }

  @Inline
  public static void removeCard(Address block, Address card) {
    Address hashtbl = RemSet.of(block);
    Address entry = CardHashTable.getEntry(hashtbl, card, false);
    if (!entry.isZero()) entry.store(Address.zero());
  }

  @Inline
  public static boolean containsCard(Address block, Address card) {
    Address hashtbl = RemSet.of(block);
    return !CardHashTable.getEntry(hashtbl, card, false).isZero();
  }

  @Inline
  private static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  public static final Offset CARDS_META_OFFSET;
  public static final Extent CARDS_META_EXTENT;
  public static final Offset REMSET_OFFSET;
  public static final Extent REMSET_EXTENT;

  static {
    // Preserve as much meta pages as possible
    MarkBlock.setAdditionalMetadataPagesPerRegion(MarkBlock.METADATA_PAGES_PER_REGION - MarkBlock.USED_METADATA_PAGES_PER_REGION);
    CARDS_META_OFFSET = Offset.zero();
    CARDS_META_EXTENT = Extent.fromIntZeroExtend(MarkBlock.BYTES_IN_BLOCK / 512);
    REMSET_OFFSET = Offset.fromIntZeroExtend(CARDS_META_EXTENT.toInt());
    REMSET_EXTENT = Extent.fromIntZeroExtend(MarkBlock.ADDITIONAL_METADATA.toInt() - REMSET_OFFSET.toInt());

    int logEntries;
    for (logEntries = 0; (1 << logEntries) < REMSET_EXTENT.toInt(); logEntries++);
    CardHashTable.LOG_ENTRIES_PER_REMSET = logEntries - 1;
  }

  static LinearScan blockLinearScan = new LinearScan() {
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
        //VM.objectModel.dumpObject(object);
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

  public static void evacuateBlock(Address block) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero());
    // Forward all objects
    MarkBlock.linearScan(blockLinearScan, block);
  }

  public static void evacuateBlocks(AddressArray relocationSet, boolean concurrent) {
    if (VM.activePlan.collector().getId() == 0) {
      for (int i = 0; i < relocationSet.length(); i++) {
        Address block = relocationSet.get(i);
        evacuateBlock(block);
      }
    }
    /*
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
    */
  }

  static TransitiveClosure redirectPointer = new TransitiveClosure() {
    @Override @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
      if (!MarkBlockSpace.Header.isMarked(source)) return;
      ObjectReference object = VM.activePlan.global().loadObjectReference(slot);
      if (!object.isNull() && Space.isInSpace(MarkCopy.MC, object)) {
        if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
          if (!ForwardingWord.isForwarded(object)) {
            Log.write("Ref ", source);
            Log.write(" . ", object);
            Log.writeln(" @ ", slot);
            MarkBlock.dumpMeta();
            VM.objectModel.dumpObject(source);
            VM.objectModel.dumpObject(object);
          }
          VM.assertions._assert(ForwardingWord.isForwarded(object));
        }
      }
    }
  };

  static LinearScan cardLinearScan = new LinearScan() {
    @Override @Uninterruptible public void scan(ObjectReference object) {
      if (MarkBlockSpace.Header.isMarked(object))
        VM.scanning.scanObject(redirectPointerTrace, object);
    }
  };

  static Lock lock = VM.newLock("abdcdgjerbejkf");
  static boolean updated = false;
  public static void updatePointersForBlock(Address block) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero());
    // Scan & correct all cards in remSet
    lock.acquire();
    if (updated) {
      lock.release();
      return;
    }
    updated = true;
    lock.release();

    //Log.writeln("> ", block);
    for (Address b = MarkCopy.markBlockSpace.firstBlock(); !b.isZero(); b = MarkCopy.markBlockSpace.nextBlock(b)) {
      //Log.write(">> ", b);
      if (!MarkBlock.relocationRequired(b)) {
        Log.writeln("Update objects in block ", b);
        MarkBlock.linearScan(cardLinearScan, b);
      }
    }
    //lock.release();
    /*
    Address remsetStart = RemSet.of(block);
    Address remsetEnd = remsetStart.plus(REMSET_EXTENT);
    for (Address cursor = remsetStart; cursor.LT(remsetEnd); cursor = cursor.plus(CardHashTable.entrySize)) {
      Address card = cursor.loadAddress();
      if (!card.isZero()) {
        MarkBlock.Card.linearScan(cardLinearScan, card);
      }
    }
    */
  }

  static public final Trace trace = new Trace(MarkCopy.metaDataSpace);
  static TraceLocal redirectPointerTrace = new TraceLocal(((MarkCopy) VM.activePlan.global()).relocateTrace) {
    @Override @UninterruptibleNoWarn public boolean isLive(ObjectReference object) {
      if (object.isNull()) return false;
      if (Space.isInSpace(MarkCopy.MC, object))
        return MarkCopy.markBlockSpace.isLive(object);
      return super.isLive(object);
    }
    @Override @Inline @UninterruptibleNoWarn public ObjectReference traceObject(ObjectReference object) {
      if (object.isNull()) return object;
      if (Space.isInSpace(MarkCopy.MC, object)) {
        if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
          if (!ForwardingWord.isForwarded(object)) VM.objectModel.dumpObject(object);
          VM.assertions._assert(ForwardingWord.isForwarded(object));
          ObjectReference newObj = MarkBlockSpace.getForwardingPointer(object);
          Log.write("Ref ", object);
          Log.writeln(" ~> ", newObj);
          return newObj;
        }
        return object;
      }
      return super.traceObject(object);
    }
  };

  public static void updatePointers(AddressArray relocationSet, boolean concurrent) {
    //if (VM.activePlan.collector().getId() == 0) {
    //trace.prepare();
    /*for (int i = 0; i < relocationSet.length(); i++) {
      Address block = relocationSet.get(i);

      Address hashtbl = RemSet.of(block);
      int entries = 1 << CardHashTable.LOG_ENTRIES_PER_REMSET;
      for (int j = 0; j < entries; i++) {
        Address card = hashtbl.plus(CardHashTable.entrySize.toInt() * j).loadAddress();
        if (!card.isZero()) {
          Log.writeln("Update pointers for card ", card);
          VM.assertions._assert(!MarkBlock.Card.getFirstObjectAddressInCard(card).isZero());
          MarkBlock.Card.linearScan(cardLinearScan, card);
        }
      }
    }
    */

    for (Address b = MarkCopy.markBlockSpace.firstBlock(); !b.isZero(); b = MarkCopy.markBlockSpace.nextBlock(b)) {
      if (!MarkBlock.relocationRequired(b)) {
        Log.write("Update objects in block ", b);
        Log.writeln(" ~ ", MarkBlock.getCursor(b));
        MarkBlock.linearScan(cardLinearScan, b);
      }
    }

    /*

      VM.scanning.computeThreadRoots(redirectPointerTrace);
      VM.scanning.computeGlobalRoots(redirectPointerTrace);
      VM.scanning.computeStaticRoots(redirectPointerTrace);
      if (Plan.SCAN_BOOT_IMAGE) {
        VM.scanning.computeBootImageRoots(redirectPointerTrace);
      }

      VM.softReferences.scan(redirectPointerTrace, false, false);
      VM.weakReferences.scan(redirectPointerTrace, false, false);
      VM.finalizableProcessor.scan(redirectPointerTrace, false);
      VM.phantomReferences.scan(redirectPointerTrace, false, false);

      VM.softReferences.forward(redirectPointerTrace, false);
      VM.weakReferences.forward(redirectPointerTrace, false);
      VM.phantomReferences.forward(redirectPointerTrace, false);
      VM.finalizableProcessor.forward(redirectPointerTrace, false);
    */

      //trace.release();
    //}


    /*int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int blocksToRelocate = ceilDiv(relocationSet.length(), workers);

    for (int i = 0; i < blocksToRelocate; i++) {
      int cursor = blocksToRelocate * id + i;
      if (cursor >= relocationSet.length()) break;
      Address block = relocationSet.get(cursor);
      updatePointersForBlock(block);
    }*/
  }
}
