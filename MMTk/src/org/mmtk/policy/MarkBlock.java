package org.mmtk.policy;

import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
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

  // Block operations

  // private static Lock blockStateLock = VM.newLock("block-state-lock");
  // private static Lock blockRegistrationLock = VM.newLock("block-registration-lock");
  // public static Lock regionReleaseLock = VM.newLock("region-release-lock");

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

  @Inline
  public static void register(Address block) {
    // blockRegistrationLock.acquire();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
    // Handle this block
    count += 1;
    clearState(block);
    setAllocated(block, true);
    // blockRegistrationLock.release();
  }

  @Inline
  public static void unregister(Address block) {
    // blockRegistrationLock.acquire();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
    count -= 1;
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
    set(metaDataOf(block, METADATA_CURSOR_OFFSET), cursor);
  }

  @Inline
  public static Address getCursor(Address block) {
    return metaDataOf(block, METADATA_CURSOR_OFFSET).loadAddress();
  }

  @Inline
  private static void clearState(Address block) {
    setAllocated(block, false);
    setRelocationState(block, false);
    setUsedSize(block, 0);
    setCursor(block, Address.zero());
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


    @Inline
    public static boolean isEnabled() { return _enabled; }
    @Inline
    public static void enable() { _enabled = true; }

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
    public static void setFirstObjectInCardIfRequired(ObjectReference ref) {
      Address address = VM.objectModel.objectStartRef(ref);
      Address card = Card.of(address);
      Address metaList = MarkBlock.additionalMetadataStart(MarkBlock.of(address));
      Address meta = metaList.plus(Card.indexOf(card));

      if (VM.VERIFY_ASSERTIONS) {
        if (!meta.LT(EmbeddedMetaData.getMetaDataBase(address).plus(METADATA_OFFSET_IN_REGION))) {
          Log.write("ObjectRef ", address);
          Log.write(" Card ", card);
          Log.write(" Index ", Card.indexOf(card));
          Log.write(" Block ", MarkBlock.of(address));
          Log.write(" Index ", MarkBlock.indexOf(address));
          Log.write(" meta start ", metaList);
          Log.write(" meta ", meta);
          Log.write(" METADATA_OFFSET_IN_REGION ", METADATA_OFFSET_IN_REGION);
          Log.write(" ADDITIONAL_METADATA ", ADDITIONAL_METADATA);
          Log.write(" ADDITIONAL_METADATA_PAGES_PER_REGION ", ADDITIONAL_METADATA_PAGES_PER_REGION);
          Log.writeln();

        }
        VM.assertions._assert(meta.LT(metaList.plus(ADDITIONAL_METADATA).minus(Constants.BYTES_IN_ADDRESS)));
        VM.assertions._assert(meta.LT(EmbeddedMetaData.getMetaDataBase(address).plus(METADATA_OFFSET_IN_REGION)));
      }
      if (meta.loadByte() == ((byte) 0)) {
        int offset = address.diff(card).toInt() / Constants.BYTES_IN_ADDRESS;
        meta.store((byte) offset);
      }
    }

    @Inline
    public static Address getFirstObjectAddressInCard(Address card) {
      Address metaList = MarkBlock.additionalMetadataStart(MarkBlock.of(card));
      Address meta = metaList.plus(Card.indexOf(card));
      int offset = meta.loadByte();
      return offset == 0 ? Address.zero() : card.plus(offset * Constants.BYTES_IN_ADDRESS);
    }

    @Inline
    public static void linearScan(LinearScan scan, Address card) {
      Address end = card.plus(MarkBlock.Card.BYTES_IN_CARD);

      Address cursor = MarkBlock.Card.getFirstObjectAddressInCard(card);
      if (cursor.isZero()) return;
      ObjectReference ref = VM.objectModel.getObjectFromStartAddress(cursor);
      /* Loop through each object up to the limit */
      do {
        /* Read end address first, as scan may be destructive */
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
