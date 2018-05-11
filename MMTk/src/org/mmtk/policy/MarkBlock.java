package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.Simple;
import org.mmtk.plan.markcopy.remset.MarkCopy;
import org.mmtk.utility.Constants;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.SimpleHashtable;
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
public class MarkBlock {
  public static final int LOG_PAGES_IN_BLOCK = 8;
  public static final int PAGES_IN_BLOCK = 1 << 8; // 256
  public static final int LOG_BYTES_IN_BLOCK = LOG_PAGES_IN_BLOCK + LOG_BYTES_IN_PAGE;
  public static final int BYTES_IN_BLOCK = 1 << LOG_BYTES_IN_BLOCK;//BYTES_IN_PAGE * PAGES_IN_BLOCK; // 1048576

  private static final Word PAGE_MASK = Word.fromIntZeroExtend(BYTES_IN_BLOCK - 1);// 0..011111111111
  public static int ADDITIONAL_METADATA_PAGES_PER_REGION = 0;
  // elements of block metadata table
  private static final int METADATA_ALIVE_SIZE_OFFSET = 0;
  private static final int METADATA_RELOCATE_OFFSET = METADATA_ALIVE_SIZE_OFFSET + BYTES_IN_INT;
  public static final int METADATA_ALLOCATED_OFFSET = METADATA_RELOCATE_OFFSET + BYTES_IN_BYTE;
  public static final int METADATA_CURSOR_OFFSET = METADATA_ALLOCATED_OFFSET + BYTES_IN_BYTE;
  //public static final int METADATA_MARK_OFFSET = METADATA_CURSOR_OFFSET + BYTES_IN_ADDRESS;
  public static final int METADATA_BYTES = 16;
  // Derived constants
  public static int METADATA_OFFSET_IN_REGION; // 0
  public static int METADATA_PAGES_PER_REGION;
  public static int BLOCKS_IN_REGION;
  public static int BLOCKS_START_OFFSET;
  public static int USED_METADATA_PAGES_PER_REGION;
  public static Extent ADDITIONAL_METADATA;

  private static void init() {
    METADATA_OFFSET_IN_REGION = ADDITIONAL_METADATA_PAGES_PER_REGION * BYTES_IN_PAGE; // 0
    int blocksInRegion = EmbeddedMetaData.PAGES_IN_REGION / PAGES_IN_BLOCK; // 1024 / 256 = 4
    int metadataBlocksInRegion = ceilDiv(8 * blocksInRegion + ADDITIONAL_METADATA_PAGES_PER_REGION * BYTES_IN_PAGE, BYTES_IN_BLOCK + 8); // 1
    BLOCKS_IN_REGION = blocksInRegion - metadataBlocksInRegion; // 3
    METADATA_PAGES_PER_REGION = metadataBlocksInRegion * PAGES_IN_BLOCK; // 256
    BLOCKS_START_OFFSET = BYTES_IN_PAGE * METADATA_PAGES_PER_REGION; // 1048576
    int metadataPages = ceilDiv(BLOCKS_IN_REGION * METADATA_BYTES, BYTES_IN_PAGE);
    USED_METADATA_PAGES_PER_REGION = metadataPages + ADDITIONAL_METADATA_PAGES_PER_REGION;
    ADDITIONAL_METADATA = Extent.fromIntZeroExtend(ADDITIONAL_METADATA_PAGES_PER_REGION * BYTES_IN_PAGE / BLOCKS_IN_REGION);
  }

  public static void dumpMeta() {
    Log.writeln("BYTES_IN_PAGE ", Constants.BYTES_IN_PAGE);
    Log.writeln("LOG_PAGES_IN_BLOCK ", LOG_PAGES_IN_BLOCK);
    Log.writeln("PAGES_IN_BLOCK ", PAGES_IN_BLOCK);
    Log.writeln("LOG_BYTES_IN_BLOCK ", LOG_BYTES_IN_BLOCK);
    Log.writeln("BYTES_IN_BLOCK ", BYTES_IN_BLOCK);
    Log.writeln("PAGE_MASK ", PAGE_MASK);
    Log.writeln("ADDITIONAL_METADATA_PAGES_PER_REGION ", ADDITIONAL_METADATA_PAGES_PER_REGION);
    Log.writeln("METADATA_ALIVE_SIZE_OFFSET ", METADATA_ALIVE_SIZE_OFFSET);
    Log.writeln("METADATA_RELOCATE_OFFSET ", METADATA_RELOCATE_OFFSET);
    Log.writeln("METADATA_ALLOCATED_OFFSET ", METADATA_ALLOCATED_OFFSET);
    Log.writeln("METADATA_CURSOR_OFFSET ", METADATA_CURSOR_OFFSET);
    Log.writeln("METADATA_BYTES ", METADATA_BYTES);
    Log.writeln("METADATA_OFFSET_IN_REGION ", METADATA_OFFSET_IN_REGION);
    Log.writeln("METADATA_PAGES_PER_REGION ", METADATA_PAGES_PER_REGION);
    Log.writeln("BLOCKS_IN_REGION ", BLOCKS_IN_REGION);
    Log.writeln("BLOCKS_START_OFFSET ", BLOCKS_START_OFFSET);
    Log.writeln("USED_METADATA_PAGES_PER_REGION ", USED_METADATA_PAGES_PER_REGION);
    Log.writeln("ADDITIONAL_METADATA ", ADDITIONAL_METADATA);
  }

  private static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  static {
    init();
  }

  public static void setAdditionalMetadataPagesPerRegion(int additionalMetadataPagesPerRegion) {
    ADDITIONAL_METADATA_PAGES_PER_REGION = additionalMetadataPagesPerRegion;
    init();
  }

  private static int count = 0;

  // Metadata setter

  @Inline
  private static void assertInMetadata(Address addr, int size) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(addr.GE(EmbeddedMetaData.getMetaDataBase(addr)));
      VM.assertions._assert(addr.plus(size).LE(EmbeddedMetaData.getMetaDataBase(addr).plus(METADATA_PAGES_PER_REGION * BYTES_IN_PAGE)));
    }
  }

  @Inline
  private static void set(Address addr, Address val) {
    assertInMetadata(addr, Constants.BYTES_IN_ADDRESS);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!addr.isZero());
    //Log.write("> ", addr);
    //Log.writeln(".store ", val);
    addr.store(val);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadAddress().EQ(val));
  }

  @Inline
  private static void set(Address addr, int val) {
    assertInMetadata(addr, Constants.BYTES_IN_INT);
    addr.store(val);
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadInt() == val);
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
  public static boolean isAligned(final Address address) {
    return address.EQ(align(address));
  }

  @Inline
  public static boolean isValidBlock(final Address block) {
    int index = indexOf(block);
    return /*block != null &&*/ !block.isZero() && isAligned(block) && index >= 0 && index < BLOCKS_IN_REGION;
  }

  @Inline
  public static int count() {
    return count;
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
  public static void setUsedSize(Address block, int bytes) {
    set(metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET), bytes);
  }

  @Inline
  public static int usedSize(Address block) {
    return metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET).loadInt();
  }

  static Lock blocksCountLock = VM.newLock("blocksCountLock");
  @Inline
  public static void register(Address block, boolean copy) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
    // Handle this block
    blocksCountLock.acquire();
    count += 1;
    blocksCountLock.release();
    clearState(block);
    setAllocated(block, true);
    if (!copy) {
      mark(block, true);
    }
  }

  @Inline
  public static void unregister(Address block) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));

    blocksCountLock.acquire();
    count -= 1;
    blocksCountLock.release();
    clearState(block);
  }

  @Inline
  public static boolean allocated(Address block) {
    return metaDataOf(block, METADATA_ALLOCATED_OFFSET).loadByte() != ((byte) 0);
  }

  private static Lock aliveSizeModifierLock = VM.newLock("alive-size-modifier-lock-since-no-cas-support");

  @Inline
  public static void updateBlockAliveSize(Address block, ObjectReference object) {
    aliveSizeModifierLock.acquire();
    setUsedSize(block, usedSize(block) + VM.objectModel.getSizeWhenCopied(object));
    aliveSizeModifierLock.release();

    /*
    int oldValue, newValue;
    do {
      oldValue = usedSize(block);
      newValue = oldValue + VM.objectModel.getSizeWhenCopied(object);
    } while (!VM.objectModel.attemptInt(metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET), Offset.fromIntZeroExtend(0), oldValue, newValue));
    */
  }

  @Inline
  public static void setCursor(Address block, Address cursor) {
    Address meta = metaDataOf(block, METADATA_CURSOR_OFFSET);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!meta.isZero());
      //VM.assertions._assert(!meta.isZero());
    }
    set(meta, cursor);
  }

  @Inline
  public static Address getCursor(Address block) {
    return metaDataOf(block, METADATA_CURSOR_OFFSET).loadAddress();
  }

  @Inline
  public static void mark(Address block, boolean state) {
    //metaDataOf(block, METADATA_MARK_OFFSET).store((byte) (state ? 1 : 0));
  }

  @Inline
  public static boolean isMarked(Address block) {
    return false;
    //return metaDataOf(block, METADATA_MARK_OFFSET).loadByte() != 0;
  }


  @Inline
  private static void clearState(Address block) {
    setAllocated(block, false);
    setRelocationState(block, false);
    setUsedSize(block, 0);
    setCursor(block, Address.zero());
    mark(block, false);
  }

  @Inline
  private static Address align(final Address ptr) {
    return ptr.toWord().and(PAGE_MASK.not()).toAddress();
  }

  @Inline
  public static int indexOf(Address block) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!block.isZero());
      VM.assertions._assert(isAligned(block));
    }
    Address region = EmbeddedMetaData.getMetaDataBase(block);
    double index = block.diff(region.plus(BLOCKS_START_OFFSET)).toInt() / BYTES_IN_BLOCK;
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index == (int) index);
    return (int) index;
  }

  public static Address additionalMetadataStart(Address block) {
    return EmbeddedMetaData.getMetaDataBase(block).plus(ADDITIONAL_METADATA.toInt() * indexOf(block));
  }

  @Inline
  private static Address metaDataOf(Address block, int metaDataOffset) {
    Address metaData = EmbeddedMetaData.getMetaDataBase(block);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(metaDataOffset >= 0 && metaDataOffset <= METADATA_BYTES);
    return metaData.plus(METADATA_OFFSET_IN_REGION + METADATA_BYTES * indexOf(block)).plus(metaDataOffset);
  }


  @Inline
  private static void setAllocated(Address block, boolean allocated) {
    //blockStateLock.acquire();
    set(metaDataOf(block, METADATA_ALLOCATED_OFFSET), (byte) (allocated ? 1 : 0));
    //blockStateLock.release();
  }

  @Uninterruptible
  public static class Card {
    private static boolean _enabled = false;
    public static final int LOG_BYTES_IN_CARD = 9;
    public static final int BYTES_IN_CARD = 1 << LOG_BYTES_IN_CARD;
    public static final Word CARD_MASK = Word.fromIntZeroExtend(BYTES_IN_CARD - 1);// 0..0111111111
    public static int[] anchors;
    public static int[] limits;

    static {
      int memorySize = VM.HEAP_END.diff(VM.HEAP_START).toInt();
      int totalCards = memorySize >> Card.LOG_BYTES_IN_CARD;
      int entries = totalCards >> 2;
      anchors = new int[entries];
      limits = new int[entries];


      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(((byte) 0xFF) == ((byte) -1));

      for (int i = 0; i < entries; i++) {
        anchors[i] = 0xFFFFFFFF;
        limits[i] = 0xFFFFFFFF;
      }
    }

    @Inline
    public static boolean compareAndSwapByteInBuffer(int[] buf, int index, byte oldByte, byte newByte) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldByte != newByte);
      int intIndex = index >> 2;
      int byteIndex = index ^ (intIndex << 2);
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(intIndex >= 0 && intIndex < buf.length);
        VM.assertions._assert(byteIndex >= 0 && byteIndex <= 3);
      }
      Offset offset = Offset.fromIntZeroExtend(intIndex << 2);
      // Get old int
      int oldValue = buf[intIndex];
      // Build new int
      int newValue = oldValue & ~(0xff << ((3 - byteIndex) << 3)); // Drop the target byte
      newValue |= (newByte << ((3 - byteIndex) << 3)); // Set new byte

      if (VM.VERIFY_ASSERTIONS) {
        if (byteIndex == 0) VM.assertions._assert((newValue << 8) == (oldValue << 8));
        if (byteIndex == 1) VM.assertions._assert((newValue << 16) == (oldValue << 16) && (newValue >>> 24) == (oldValue >>> 24));
        if (byteIndex == 2) VM.assertions._assert((newValue << 24) == (oldValue << 24) && (newValue >>> 16) == (oldValue >>> 16));
        if (byteIndex == 3) VM.assertions._assert((newValue >>> 8) == (oldValue >>> 8));
      }
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldValue != newValue);
      if (oldValue == newValue) return false;
      return VM.objectModel.attemptInt(buf, offset, oldValue, newValue);
    }

    @Inline
    public static byte getByte(int[] buf, int index) {
      int intIndex = index >> 2;
      int byteIndex = index ^ (intIndex << 2);
      int entry = buf[intIndex];
      return (byte) ((entry << (byteIndex << 3)) >> 24);
    }

    @Inline
    public static boolean isEnabled() { return _enabled; }

    @Inline
    public static void enable() { _enabled = true; }

    @Inline
    public static boolean isAligned(Address card) {
      return card.toWord().and(CARD_MASK.not()).toAddress().EQ(card);
    }

    @Inline
    public static Address of(Address address) {
      return address.toWord().and(CARD_MASK.not()).toAddress();
    }

    @Inline
    public static int indexOf(Address card) {
      Address block = MarkBlock.of(card);
      int index = card.diff(block).toInt() / BYTES_IN_CARD;
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index >= 0);
      return index;
    }

    @Inline
    public static int hash(Address card) {
      return card.diff(VM.HEAP_START).toInt() >> Card.LOG_BYTES_IN_CARD;
    }

    static Lock lock = VM.newLock("asfghgjnxcsae");

    @Inline
    public static void updateCardMeta(ObjectReference ref) {
      /*Log.write("Update ");
      Log.write(Space.getSpaceForObject(ref).getName());
      Log.write(" object ", ref);
      Log.write(" (", VM.objectModel.objectStartRef(ref));
      Log.write(", ", VM.objectModel.getObjectEndAddress(ref));
      Log.writeln(")");*/
      //lock.acquire();

      // set anchor value
      final Address objectStartAddress = VM.objectModel.objectStartRef(ref);
      final Address card = Card.of(objectStartAddress);
      final int cardIndex = hash(card);
      /*if (Space.isInSpace(Plan.VM_SPACE, ref)) {
        Log.write("Update vm-space object ", ref);
        Log.write(" (", VM.objectModel.objectStartRef(ref));
        Log.write(", ", VM.objectModel.getObjectEndAddress(ref));
        Log.writeln(") {");
        Log.write("  Old card ", card);
        Log.write(" range (", getByte(anchors, cardIndex));
        Log.write(", ", getByte(limits, cardIndex));
        Log.writeln(")");
      }*/

      // CAS anchor value
      byte oldStartOffset, newStartOffset;
      do {
        // Get old value
        oldStartOffset = getByte(anchors, cardIndex);
        // Build new value
        newStartOffset = (byte) (objectStartAddress.diff(card).toInt() >> Constants.LOG_BYTES_IN_ADDRESS);
        // Break if (old != -1 && old <= new)
        if (oldStartOffset != ((byte) -1) && oldStartOffset <= newStartOffset) break;
        /*if (Space.isInSpace(Plan.VM_SPACE, ref)) {
          Log.write("  Update card ", card);
          Log.writeln(" start ", newStartOffset);
        }*/
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
          if (VM.VERIFY_ASSERTIONS) {
            VM.assertions._assert(objectEndAddress.LT(card.plus(BYTES_IN_CARD)));
            VM.assertions._assert(newStartOffset >= 0);
          }
          newEndOffset = (byte) (objectEndAddress.diff(card).toInt() >> Constants.LOG_BYTES_IN_ADDRESS);
        } else {
          if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(endCard.GT(card));
          newEndOffset = (byte) 0;
        }
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(newEndOffset >= 0);
        // Break if old >= new or oldEndOffset is already 0
        if (oldEndOffset == 0) break;
        if (newEndOffset != 0 && oldEndOffset >= newEndOffset) break;
        //Log.write("Card ", card);
        //Log.writeln(" end ", endOffset);
        /*if (Space.isInSpace(Plan.VM_SPACE, ref)) {
          Log.write("  Update card ", card);
          Log.writeln(" end to ", newEndOffset);
        }*/
      } while (!compareAndSwapByteInBuffer(limits, cardIndex, oldEndOffset, newEndOffset));

      /*if (Space.isInSpace(Plan.VM_SPACE, ref)) {
        Log.write("  Card ", card);
        Log.write(" range (", getByte(anchors, cardIndex));
        Log.write(", ", getByte(limits, cardIndex));
        Log.writeln(")");
        Log.writeln("}");
      }*/

      /*byte b = getByte(limits, hash(Address.fromIntZeroExtend(0x68008200)));
      if (!((b == (byte) 122) || (b == (byte) -1))) {
        Log.writeln("!!! ", b);
        Log.writeln("cardIndex ", cardIndex);
      }
      VM.assertions._assert( (b == (byte) 122) || (b == (byte) -1));
      */
      //lock.release();
    }

    @Inline
    public static Address getCardAnchor(Address card) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Card.of(card)));
      int cardIndex = hash(card);
      byte offset = getByte(anchors, cardIndex);
      if (offset == (byte) -1) return Address.zero();
      return card.plus(offset << Constants.LOG_BYTES_IN_ADDRESS);
    }

    @Inline
    public static Address getCardLimit(Address card) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Card.of(card)));
      int cardIndex = hash(card);
      byte offset = getByte(limits, cardIndex);
      if (offset == (byte) -1) return Address.zero(); // limit is unset
      return card.plus(offset == 0 ? BYTES_IN_CARD : (offset << Constants.LOG_BYTES_IN_ADDRESS));
    }

    @Inline
    public static void clearCardMeta(Address card) {
      int i = hash(card);
      byte oldByte;

      do {
        oldByte = getByte(anchors, i);
        if (oldByte == (byte) -1) break;
      } while (!compareAndSwapByteInBuffer(anchors, i, oldByte, (byte) -1));

      do {
        oldByte = getByte(limits, i);
        if (oldByte == (byte) -1) break;
      } while (!compareAndSwapByteInBuffer(limits, i, oldByte, (byte) -1));
    }

    @Inline
    public static void clearCardMetaForBlock(Address block) {
      Address end = block.plus(MarkBlock.BYTES_IN_BLOCK);
      for (Address c = block; c.LT(end); c = c.plus(Card.BYTES_IN_CARD)) {
        clearCardMeta(c);
      }
    }

    @Inline
    public static void clearAllCardMeta(boolean concurrent) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int totalCards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >> LOG_BYTES_IN_CARD;
      int cardsToClear = ceilDiv(totalCards, workers);

      for (int i = 0; i < cardsToClear; i++) {
        int index = cardsToClear * id + i;
        if (index >= totalCards) break;
        Address c = VM.HEAP_START.plus(index << LOG_BYTES_IN_CARD);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Card.isAligned(c));
        clearCardMeta(c);
      }
    }

    @Inline
    public static void linearScan(LinearScan scan, Address card) {
      linearScan(scan, card, false);
    }
    @Inline
    public static void linearScan(LinearScan scan, Address card, boolean log) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!Space.isInSpace(Plan.VM_SPACE, card));
      Address end = getCardLimit(card);
      if (end.isZero()) return;
      Address cursor = MarkBlock.Card.getCardAnchor(card);
      if (cursor.isZero()) return;
      ObjectReference ref = VM.objectModel.getObjectFromStartAddress(cursor);
      if (log) {
        Log.write("Linear scan card ", card);
        Log.write(", range ", MarkBlock.Card.getCardAnchor(card));
        Log.write(" ..< ", MarkBlock.Card.getCardLimit(card));
        Log.write(", offsets ", MarkBlock.Card.getByte(MarkBlock.Card.anchors, MarkBlock.Card.hash(card)));
        Log.write(" ..< ", MarkBlock.Card.getByte(MarkBlock.Card.limits, MarkBlock.Card.hash(card)));
        Log.write(" in space: ");
        Log.writeln(Space.getSpaceForAddress(card).getName());
      }
      do {
        if (ref.isNull()) break;

        if (log) {
          VM.objectModel.dumpObject(ref);
        }

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

          /*if (currentCell.NE(VM.objectModel.objectStartRef(ref))) {
            VM.objectModel.dumpObject(ref);
            Log.writeln("Size ", VM.objectModel.getCurrentSize(ref));
            Log.writeln("SizeWhenCopy ", VM.objectModel.getSizeWhenCopied(ref));
            Log.writeln("BlockSize ", BlockAllocator.blockSize(BlockAllocator.getBlkSizeClass(ref.toAddress())));
            Log.write("!!! ", currentCell);
            Log.write(" vs ", VM.objectModel.objectStartRef(ref));
            Log.write(", block ", block);
            Log.write(" first cell ", firstCell);
            Log.write(" cellSizeClass ", cellSizeClass);
            Log.write(" cellExtent ", cellExtent);
            Log.writeln(" cellIndex ", cellIndex);
            VM.memory.dumpMemory(VM.objectModel.objectStartRef(ref), 0, VM.objectModel.getCurrentSize(ref) + 30);
            if (VM.objectModel.objectStartRef(ref).EQ(Address.fromIntZeroExtend(0x6801a014))) {
              ObjectReference ref2 = VM.objectModel.getObjectFromStartAddress(Address.fromIntZeroExtend(0x6801a010));
              VM.objectModel.dumpObject(ref2);
            }
          }
          VM.assertions._assert(currentCell.EQ(VM.objectModel.objectStartRef(ref)));
          */
          // Get next freelist start address
          Address nextCell = currentCell.plus(cellExtent);
          //
          currentObjectEnd = nextCell;//.plus(Constants.BYTES_IN_ADDRESS);
          //Log.writeln(" -> ", currentObjectEnd);
        } else {
          currentObjectEnd = VM.objectModel.getObjectEndAddress(ref);
        }

        scan.scan(ref);

        if (currentObjectEnd.GE(end)) {
          break;
        }

        ObjectReference next = VM.objectModel.getObjectFromStartAddress(currentObjectEnd);
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(next.toAddress().GT(ref.toAddress()));
        }
        ref = next;
      } while (true);
    }
  }

  @Inline
  public static void linearScan(LinearScan scan, Address block) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));

    Address end = getCursor(block);

    Address cursor = block;
    if (cursor.GE(end)) return;
    ObjectReference ref = VM.objectModel.getObjectFromStartAddress(cursor);
    /* Loop through each object up to the limit */
    do {
      /* Read end address first, as scan may be destructive */
      //Log.write("Block ", block);
      //Log.write(" ");
      //VM.objectModel.dumpObject(ref);
      Address currentObjectEnd = VM.objectModel.getObjectEndAddress(ref);
      scan.scan(ref);
      //VM.scanning.scanObject(scanPointers, ref);
      if (currentObjectEnd.GE(end)) {
        /* We have scanned the last object */
        break;
      }
      /* Find the next object from the start address (dealing with alignment gaps, etc.) */
      ObjectReference next = VM.objectModel.getObjectFromStartAddress(currentObjectEnd);
      if (VM.VERIFY_ASSERTIONS) {
        /* Must be monotonically increasing */
        VM.assertions._assert(next.toAddress().GT(ref.toAddress()));
      }
      ref = next;
    } while (true);
  }
}
