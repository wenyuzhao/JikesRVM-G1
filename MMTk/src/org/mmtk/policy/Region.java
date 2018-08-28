package org.mmtk.policy;

import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BlockAllocator;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class Region {
  public static final int NORMAL = 0;
  public static final int EDEN = NORMAL;
  public static final int SURVIVOR = 1;
  public static final int OLD = 2;

  public static final int LOG_PAGES_IN_BLOCK = 8;
  public static final int PAGES_IN_BLOCK = 1 << 8; // 256
  public static final int LOG_BYTES_IN_BLOCK = LOG_PAGES_IN_BLOCK + LOG_BYTES_IN_PAGE;
  public static final int BYTES_IN_BLOCK = 1 << LOG_BYTES_IN_BLOCK;//BYTES_IN_PAGE * PAGES_IN_BLOCK; // 1048576

  private static final Word PAGE_MASK = Word.fromIntZeroExtend(BYTES_IN_BLOCK - 1);// 0..011111111111
  // elements of block metadata table
  public static final int METADATA_ALIVE_SIZE_OFFSET = 0;
  public static final int METADATA_RELOCATE_OFFSET = METADATA_ALIVE_SIZE_OFFSET + BYTES_IN_INT;
  public static final int METADATA_ALLOCATED_OFFSET = METADATA_RELOCATE_OFFSET + BYTES_IN_BYTE;
  public static final int METADATA_CURSOR_OFFSET = METADATA_ALLOCATED_OFFSET + BYTES_IN_BYTE;
  public static final int METADATA_REMSET_LOCK_OFFSET = METADATA_CURSOR_OFFSET + BYTES_IN_ADDRESS;
  public static final int METADATA_REMSET_SIZE_OFFSET = METADATA_REMSET_LOCK_OFFSET + BYTES_IN_INT;
  public static final int METADATA_REMSET_PAGES_OFFSET = METADATA_REMSET_SIZE_OFFSET + BYTES_IN_INT;
  public static final int METADATA_REMSET_POINTER_OFFSET = METADATA_REMSET_PAGES_OFFSET + BYTES_IN_INT;
  public static final int METADATA_GENERATION_OFFSET = METADATA_REMSET_POINTER_OFFSET + BYTES_IN_ADDRESS;
//  public static final int METADATA_BYTES = METADATA_GENERATION_OFFSET + BYTES_IN_INT;
  public static final int METADATA_FORWARDING_TABLE_OFFSET = METADATA_GENERATION_OFFSET + BYTES_IN_INT;
  public static final int METADATA_BYTES = METADATA_FORWARDING_TABLE_OFFSET + BYTES_IN_ADDRESS;

  // Derived constants
  public static final int METADATA_OFFSET_IN_REGION = 0; // 0
  public static final int METADATA_BLOCKS_PER_REGION;
  public static final int METADATA_PAGES_PER_REGION;
  public static final int BLOCKS_IN_REGION;
  public static final int BLOCKS_START_OFFSET;
  public static final int MARKING_METADATA_START;
  public static final int MARKING_METADATA_EXTENT;

  static {
    int blocksInRegion = EmbeddedMetaData.PAGES_IN_REGION / PAGES_IN_BLOCK; // 1024 / 256 = 4
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(blocksInRegion == 32);
    int metadataBlocksInRegion = ceilDiv(blocksInRegion * METADATA_BYTES, BYTES_IN_BLOCK); // 1
    BLOCKS_IN_REGION = blocksInRegion - metadataBlocksInRegion; // 3
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(BLOCKS_IN_REGION == 31);
    METADATA_BLOCKS_PER_REGION = metadataBlocksInRegion; // 256
    METADATA_PAGES_PER_REGION = metadataBlocksInRegion * PAGES_IN_BLOCK; // 256
    BLOCKS_START_OFFSET = BYTES_IN_PAGE * METADATA_PAGES_PER_REGION; // 1048576
    MARKING_METADATA_START = METADATA_BYTES * BLOCKS_IN_REGION;
    MARKING_METADATA_EXTENT = BYTES_IN_BLOCK * metadataBlocksInRegion - MARKING_METADATA_START;
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(MARKING_METADATA_START < (METADATA_PAGES_PER_REGION * BYTES_IN_PAGE));
    }
  }

  public static void dumpMeta() {
    Log.writeln("BYTES_IN_PAGE ", Constants.BYTES_IN_PAGE);
    Log.writeln("LOG_PAGES_IN_BLOCK ", LOG_PAGES_IN_BLOCK);
    Log.writeln("PAGES_IN_BLOCK ", PAGES_IN_BLOCK);
    Log.writeln("LOG_BYTES_IN_BLOCK ", LOG_BYTES_IN_BLOCK);
    Log.writeln("BYTES_IN_BLOCK ", BYTES_IN_BLOCK);
    Log.writeln("PAGE_MASK ", PAGE_MASK);
    Log.writeln("METADATA_ALIVE_SIZE_OFFSET ", METADATA_ALIVE_SIZE_OFFSET);
    Log.writeln("METADATA_RELOCATE_OFFSET ", METADATA_RELOCATE_OFFSET);
    Log.writeln("METADATA_ALLOCATED_OFFSET ", METADATA_ALLOCATED_OFFSET);
    Log.writeln("METADATA_CURSOR_OFFSET ", METADATA_CURSOR_OFFSET);
    Log.writeln("METADATA_REMSET_LOCK_OFFSET ", METADATA_REMSET_LOCK_OFFSET);
    Log.writeln("METADATA_REMSET_SIZE_OFFSET ", METADATA_REMSET_SIZE_OFFSET);
    Log.writeln("METADATA_REMSET_PAGES_OFFSET ", METADATA_REMSET_PAGES_OFFSET);
    Log.writeln("METADATA_REMSET_POINTER_OFFSET ", METADATA_REMSET_POINTER_OFFSET);
    Log.writeln("METADATA_BYTES ", METADATA_BYTES);
    Log.writeln("MARKING_METADATA_START ", MARKING_METADATA_START);
    Log.writeln("MARKING_METADATA_EXTENT ", MARKING_METADATA_EXTENT);
    Log.writeln("METADATA_OFFSET_IN_REGION ", METADATA_OFFSET_IN_REGION);
    Log.writeln("METADATA_PAGES_PER_REGION ", METADATA_PAGES_PER_REGION);
    Log.writeln("METADATA_PAGES_PER_REGION ", METADATA_PAGES_PER_REGION);
    Log.writeln("BLOCKS_IN_REGION ", BLOCKS_IN_REGION);
    Log.writeln("BLOCKS_START_OFFSET ", BLOCKS_START_OFFSET);
    //VM.assertions.fail("");
  }

  private static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  // Metadata setter

  @Inline
  private static void assertInMetadata(Address addr, int size) {
    if (VM.VERIFY_ASSERTIONS) {
      Address base = EmbeddedMetaData.getMetaDataBase(addr);
      VM.assertions._assert(addr.GE(base));
      VM.assertions._assert(addr.plus(size).LE(base.plus(METADATA_BYTES * BYTES_IN_BLOCK)));
    }
  }

  @Inline
  private static void set(Address addr, Address val) {
    assertInMetadata(addr, Constants.BYTES_IN_ADDRESS);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!addr.isZero());
    addr.store(val);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadAddress().EQ(val));
  }

  @Inline
  private static void set(Address addr, int val) {
    assertInMetadata(addr, Constants.BYTES_IN_INT);
    addr.store(val);
  }

  @Inline
  private static void set(Address addr, byte val) {
    assertInMetadata(addr, Constants.BYTES_IN_BYTE);
    addr.store(val);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadByte() == val);
  }

  @Inline
  public static Address of(final Address ptr) {
    return align(ptr);
  }

  @Inline
  public static Address of(final ObjectReference ref) {
    return of(VM.objectModel.objectStartRef(ref));
  }

  @Inline
  public static boolean isAligned(final Address address) {
    return address.EQ(align(address));
  }

  @Inline
  public static boolean isValidBlock(final Address block) {
    int index = indexOf(block);
    return /*block != null &&*/ !block.isZero() && isAligned(block) && index >= 0 && index < BLOCKS_IN_REGION;
  }

  @Inline
  public static void setRelocationState(Address block, boolean relocation) {
    // blockStateLock.acquire();
    set(metaDataOf(block, METADATA_RELOCATE_OFFSET), (byte) (relocation ? 1 : 0));
    // blockStateLock.release();
  }

  @Inline
  public static boolean relocationRequired(Address block) {
    return metaDataOf(block, METADATA_RELOCATE_OFFSET).loadByte() != ((byte) 0);
  }

  @Inline
  public static void clearMarkBitMap(Address block) {
    Address start = EmbeddedMetaData.getMetaDataBase(block).plus(MARKING_METADATA_START);
    VM.memory.zero(false, start, Extent.fromIntZeroExtend(MARKING_METADATA_EXTENT));
  }

  @Inline
  public static void setUsedSize(Address block, int bytes) {
    set(metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET), bytes);
  }

  @Inline
  public static int usedSize(Address block) {
    return metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET).loadInt();
  }

  static Lock blocksCountLock = VM.newLock("blocksCountLock");

  @Inline
  public static int kind(Address region) {
    return Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt();
  }

  @Inline
  public static void register(Address block, int allocationKind) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
    // Handle this block
    //blocksCountLock.acquire();
    //count += 1;
    //blocksCountLock.release();
    clearState(block);
    setAllocated(block, true);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(allocationKind >= 0 && allocationKind <= 2);
    }
    Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).store(allocationKind);
  }

  @Inline
  public static void unregister(Address block) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
    //blocksCountLock.acquire();
    //count -= 1;
    //blocksCountLock.release();
    clearState(block);
  }

  @Inline
  public static boolean allocated(Address block) {
    return metaDataOf(block, METADATA_ALLOCATED_OFFSET).loadByte() != ((byte) 0);
  }

  @Inline
  public static void updateBlockAliveSize(Address block, ObjectReference object) {
    Address meta = metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET);
    int oldValue, newValue, size = VM.objectModel.getSizeWhenCopied(object);
    do {
      oldValue = meta.prepareInt();
      newValue = oldValue + size;
    } while (!meta.attempt(oldValue, newValue));
  }

  @Inline
  public static void setCursor(Address block, Address cursor) {
    Address meta = metaDataOf(block, METADATA_CURSOR_OFFSET);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!meta.isZero());
    }
    set(meta, cursor);
  }

  @Inline
  public static Address getCursor(Address block) {
    return metaDataOf(block, METADATA_CURSOR_OFFSET).loadAddress();
  }

  @Inline
  private static void clearState(Address region) {
    Address metaData = EmbeddedMetaData.getMetaDataBase(region);
    Address metaForRegion = metaData.plus(METADATA_OFFSET_IN_REGION + METADATA_BYTES * indexOf(region));
    // Forwarding table should not be cleared
    Address forwardingTable = metaDataOf(region, METADATA_FORWARDING_TABLE_OFFSET).loadAddress();
    VM.memory.zero(false, metaForRegion, Extent.fromIntZeroExtend(METADATA_BYTES));
    metaDataOf(region, METADATA_FORWARDING_TABLE_OFFSET).store(forwardingTable);
  }

  @Inline
  private static Address align(final Address ptr) {
    return ptr.toWord().and(PAGE_MASK.not()).toAddress();
  }

  @Inline
  public static int indexOf(Address block) {
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(!block.isZero());
//      VM.assertions._assert(isAligned(block));
//    }
    Address region = EmbeddedMetaData.getMetaDataBase(block);
    int index = block.diff(region.plus(BLOCKS_START_OFFSET)).toWord().rshl(LOG_BYTES_IN_BLOCK).toInt();
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index == (int) index);
    return index;
  }

  @Inline
  public static Address metaDataOf(Address block, int metaDataOffset) {
    Address metaData = EmbeddedMetaData.getMetaDataBase(block);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(metaDataOffset >= 0 && metaDataOffset <= METADATA_BYTES);
    return metaData.plus(METADATA_OFFSET_IN_REGION + METADATA_BYTES * indexOf(block)).plus(metaDataOffset);
  }


  @Inline
  private static void setAllocated(Address block, boolean allocated) {
    set(metaDataOf(block, METADATA_ALLOCATED_OFFSET), (byte) (allocated ? 1 : 0));
  }

  public static boolean USE_CARDS = false;

  @Uninterruptible
  public static class Card {
    public static final int LOG_BYTES_IN_CARD = 9;
    public static final int BYTES_IN_CARD = 1 << LOG_BYTES_IN_CARD;
    public static final Word CARD_MASK = Word.fromIntZeroExtend(BYTES_IN_CARD - 1);// 0..0111111111
    public static int[] anchors;
    public static int[] limits;

    static {
      if (USE_CARDS) {
        int memorySize = VM.HEAP_END.diff(VM.HEAP_START).toInt();
        int totalCards = memorySize >>> Card.LOG_BYTES_IN_CARD;
        int entries = totalCards >>> 2;
        anchors = new int[entries];
        limits = new int[entries];

        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(((byte) 0xFF) == ((byte) -1));

        for (int i = 0; i < entries; i++) {
          anchors[i] = 0xFFFFFFFF;
          limits[i] = 0xFFFFFFFF;
        }
      }
    }

    @Inline
    public static boolean compareAndSwapByteInBuffer(int[] buf, int index, byte oldByte, byte newByte) {
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldByte != newByte);
      int intIndex = index >>> 2;
      int byteIndex = index ^ (intIndex << 2);
//      if (VM.VERIFY_ASSERTIONS) {
//        VM.assertions._assert(intIndex == index / 4 && byteIndex == index % 4);
//      }
      Offset offset = Offset.fromIntZeroExtend(intIndex << 2);
      // Get old int
      int oldValue = buf[intIndex];
      // Build new int
      int newValue = oldValue & ~(0xff << ((3 - byteIndex) << 3)); // Drop the target byte
      newValue |= ((((int) newByte) & 0xFF) << ((3 - byteIndex) << 3)); // Set new byte

//      if (VM.VERIFY_ASSERTIONS) {
//        if (byteIndex == 0) VM.assertions._assert((newValue << 8) == (oldValue << 8));
//        if (byteIndex == 1) VM.assertions._assert((newValue << 16) == (oldValue << 16) && (newValue >>> 24) == (oldValue >>> 24));
//        if (byteIndex == 2) VM.assertions._assert((newValue << 24) == (oldValue << 24) && (newValue >>> 16) == (oldValue >>> 16));
//        if (byteIndex == 3) VM.assertions._assert((newValue >>> 8) == (oldValue >>> 8));
//      }
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldValue != newValue);
      if (oldValue == newValue) return false;
      return VM.objectModel.attemptInt(buf, offset, oldValue, newValue);
    }

    @Inline
    public static byte getByte(int[] buf, int index) {
      int intIndex = index >>> 2;
      int byteIndex = index ^ (intIndex << 2);
      int entry = buf[intIndex];
      return (byte) ((entry << (byteIndex << 3)) >>> 24);
    }

    @Inline
    public static boolean isAligned(Address card) {
      return card.toWord().and(CARD_MASK.not()).toAddress().EQ(card);
    }

    @Inline
    public static Address of(Address address) {
      return address.toWord().and(CARD_MASK.not()).toAddress();
    }

    @Inline
    public static Address of(ObjectReference ref) {
      return of(VM.objectModel.objectStartRef(ref));
    }

    @Inline
    public static int indexOf(Address card) {
      Address block = Region.of(card);
      int index = card.diff(block).toInt() >>> LOG_BYTES_IN_CARD;
      return index;
    }

    @Inline
    public static int hash(Address card) {
      return card.diff(VM.HEAP_START).toInt() >>> Card.LOG_BYTES_IN_CARD;
    }

    static Lock lock = VM.newLock("asfghgjnxcsae");

    @Inline
    public static void updateCardMeta(ObjectReference ref) {
      // set anchor value
      final Address objectStartAddress = VM.objectModel.objectStartRef(ref);
      final Address card = Card.of(objectStartAddress);
      final int cardIndex = hash(card);
      if ((cardIndex >>> 2) >= anchors.length) {
        Log.write("Overflow cardIndex=", cardIndex);
        Log.write(" card=", card);
        Log.write(" ref=", ref);
        Log.writeln(" length=", anchors.length);
      }
      // CAS anchor value
      byte oldStartOffset, newStartOffset;
      do {
        // Get old value
        oldStartOffset = getByte(anchors, cardIndex);
        // Build new value
        newStartOffset = (byte) (objectStartAddress.diff(card).toInt() >>> Constants.LOG_BYTES_IN_ADDRESS);
        // Break if (old != -1 && old <= new)
        if (oldStartOffset != ((byte) -1) && oldStartOffset <= newStartOffset) break;
      } while (!compareAndSwapByteInBuffer(anchors, cardIndex, oldStartOffset, newStartOffset));

      // set limit value
      final Address objectEndAddress = VM.objectModel.getObjectEndAddress(ref);
      final Address endCard = Card.of(objectEndAddress);

      // CAS limit value
      byte oldEndOffset, newEndOffset;
      do {
        // Get old value
        oldEndOffset = getByte(limits, cardIndex);
        // Build new value
        if (endCard.EQ(card)) {
          newEndOffset = (byte) (objectEndAddress.diff(card).toInt() >>> Constants.LOG_BYTES_IN_ADDRESS);
        } else {
          newEndOffset = (byte) 0;
        }
        // Break if old >= new or oldEndOffset is already 0
        if (oldEndOffset == 0) break;
        if (newEndOffset != 0 && oldEndOffset >= newEndOffset) break;
      } while (!compareAndSwapByteInBuffer(limits, cardIndex, oldEndOffset, newEndOffset));
    }

    @Inline
    public static Address getCardAnchor(Address card) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Card.of(card)));
      int cardIndex = hash(card);
      byte offset = getByte(anchors, cardIndex);
      if (offset == (byte) -1) return Address.zero();
      return card.plus(offset << Constants.LOG_BYTES_IN_ADDRESS);
    }

    @Inline
    public static Address getCardLimit(Address card) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Card.of(card)));
      int cardIndex = hash(card);
      byte offset = getByte(limits, cardIndex);
      if (offset == (byte) -1) return Address.zero(); // limit is unset
      return card.plus(offset == 0 ? BYTES_IN_CARD : (offset << Constants.LOG_BYTES_IN_ADDRESS));
    }

    @Inline
    public static void clearCardMeta(Address card) {
      int i = hash(card);
      ObjectReference.fromObject(anchors).toAddress().plus(i).store((byte) -1);
      ObjectReference.fromObject(limits).toAddress().plus(i).store((byte) -1);
//      byte oldByte;
//
//      do {
//        oldByte = getByte(anchors, i);
//        if (oldByte == (byte) -1) break;
//      } while (!compareAndSwapByteInBuffer(anchors, i, oldByte, (byte) -1));
//
//      do {
//        oldByte = getByte(limits, i);
//        if (oldByte == (byte) -1) break;
//      } while (!compareAndSwapByteInBuffer(limits, i, oldByte, (byte) -1));
    }

    @Inline
    public static void clearCardMetaForBlock(Address block) {
      Address end = block.plus(Region.BYTES_IN_BLOCK);
      for (Address c = block; c.LT(end); c = c.plus(Card.BYTES_IN_CARD)) {
        clearCardMeta(c);
      }
    }

    @Inline
    public static void clearCardMetaForUnmarkedCards(RegionSpace regionSpace, boolean concurrent, boolean nursery) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int totalEntries = anchors.length;
      int entriesToClear = ceilDiv(anchors.length, workers);

      int G1_SPACE = regionSpace.getDescriptor();

      for (int i = 0; i < entriesToClear; i++) {
        int index = entriesToClear * id + i;
        if (index >= totalEntries) break;
        int firstCardIndex = index << Constants.LOG_BYTES_IN_INT;
        Address firstCard = VM.HEAP_START.plus(firstCardIndex << LOG_BYTES_IN_CARD);
        if (!Space.isMappedAddress(firstCard)) {
          anchors[index] = 0xFFFFFFFF;
          limits[index] = 0xFFFFFFFF;
          continue;
        }
        if (Space.isInSpace(G1_SPACE, firstCard) && !Region.allocated(Region.of(firstCard))) {
          anchors[index] = 0xFFFFFFFF;
          limits[index] = 0xFFFFFFFF;
          continue;
        }
//        if (nursery) continue;
        if (Space.isInSpace(G1_SPACE, firstCard)) continue;

        Space space = Space.getSpaceForAddress(firstCard);
        if (space instanceof SegregatedFreeListSpace) {
          if (!BlockAllocator.checkBlockMeta(firstCard)) {
            anchors[index] = 0xFFFFFFFF;
            limits[index] = 0xFFFFFFFF;
          }
        } if (space instanceof LargeObjectSpace) {
          if (!((LargeObjectSpace) space).isInToSpace(firstCard)) {
            anchors[index] = 0xFFFFFFFF;
            limits[index] = 0xFFFFFFFF;
          }
        }

//          if (Space.isInSpace(G1_SPACE, firstCard)) continue;
//          anchors[index] = 0xFFFFFFFF;
//          limits[index] = 0xFFFFFFFF;
      }
    }
    public static String tag = null;

    static Lock lock2 = VM.newLock("linearScan");
    public static boolean LOG = false;
    public static boolean DISABLE_DYNAMIC_HASH_OFFSET = false;
    public static final Offset OBJECT_END_ADDRESS_OFFSET = VM.objectModel.GC_HEADER_OFFSET().plus(Constants.BYTES_IN_ADDRESS);

    @Inline
    @Uninterruptible
    public static void linearScan(LinearScan scan, RegionSpace regionSpace, Address card, boolean skipDeadBlock) {
      final int RS = regionSpace.getDescriptor();

      Address end = getCardLimit(card);
      if (end.isZero()) return;
      Address cursor = Region.Card.getCardAnchor(card);
      if (cursor.isZero()) return;
      ObjectReference ref = VM.objectModel.getObjectFromStartAddress(cursor);
//      int space = Space.getSpaceForAddress(card).getDescriptor();
//      Space space = Space.getSpaceForAddress(card);
//      if (space instanceof SegregatedFreeListSpace) {
//        if (!BlockAllocator.checkBlockMeta(card)) {
//          return;
//        }
//      }
//      if (space instanceof LargeObjectSpace) {
//        if (!((LargeObjectSpace) space).isInToSpace(firstCard)) {
//          return;
//        }
//      }

      if (Space.isInSpace(RS, card)) {
        Address regionLimit = Region.metaDataOf(Region.of(card), Region.METADATA_CURSOR_OFFSET).loadAddress();
        end = regionLimit.LT(end) ? regionLimit : end;
        if (end.LT(cursor)) return;
      }

      do {
        // Get next object start address, i.e. current object end address
        Address currentObjectEnd;
        if (Space.getSpaceForAddress(card) instanceof MarkSweepSpace) {
          // VM.objectModel.dumpObject(ref);
          MarkSweepSpace space = (MarkSweepSpace) Space.getSpaceForAddress(card);
          // Get current block
          Address block = BlockAllocator.getBlkStart(ref.toAddress());
          // Get cell size classes
          byte cellSizeClass = BlockAllocator.getClientSizeClass(VM.objectModel.objectStartRef(ref));
          // Get first cell address
          Address firstCell = block.plus(space.getBlockHeaderSize(cellSizeClass));//BlockAllocator.getFreeListMeta(block);
          // Get cell extent
          int cellExtent = space.getBaseCellSize(cellSizeClass);
          // Get current cell for `ref`
          int cellIndex = VM.objectModel.objectStartRef(ref).diff(firstCell).toInt() / cellExtent;
          Address currentCell = firstCell.plus(cellExtent * cellIndex);
          // Get next freelist start address
          Address nextCell = currentCell.plus(cellExtent);
          //
          currentObjectEnd = nextCell;//.plus(Constants.BYTES_IN_ADDRESS);
        } else {
          if (!ref.toAddress().loadWord(OBJECT_END_ADDRESS_OFFSET).isZero()) {
            currentObjectEnd = ref.toAddress().loadWord(OBJECT_END_ADDRESS_OFFSET).toAddress();
          } else {
            currentObjectEnd = VM.objectModel.getObjectEndAddress(ref);
          }
        }

        if (currentObjectEnd.GE(end)) {
//          if (ref.toAddress().loadWord(LIVE_STATE_OFFSET).isZero()) {
            scan.scan(ref);
//          }
          break;
        } else {
          ObjectReference next = VM.objectModel.getObjectFromStartAddress(currentObjectEnd);
//          if (ref.toAddress().loadWord(LIVE_STATE_OFFSET).isZero()) {
            scan.scan(ref);
//          }
          ref = next;
        }
      } while (true);
    }
  }

  @Inline
  public static void linearScan(LinearScan scan, Address block) {
    Address end = getCursor(block);
    Address cursor = block;
    if (cursor.GE(end)) return;
    ObjectReference ref = VM.objectModel.getObjectFromStartAddress(cursor);
    do {
      Address currentObjectEnd = VM.objectModel.getObjectEndAddress(ref);
      scan.scan(ref);
      if (currentObjectEnd.GE(end)) {
        break;
      }
      ref = VM.objectModel.getObjectFromStartAddress(currentObjectEnd);
    } while (true);
  }
}
