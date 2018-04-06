package org.mmtk.policy;

import org.mmtk.utility.Constants;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Word;

import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class MarkBlock {
  public static final int PAGES_IN_BLOCK = 1;
  public static final int BYTES_IN_BLOCK = BYTES_IN_PAGE * PAGES_IN_BLOCK;

  public static final int METADATA_OFFSET_IN_REGION = 0;//BLOCK_COUNT_OFFSET_IN_REGION + Constants.BYTES_IN_INT;

  private static final int METADATA_ALIVE_SIZE_OFFSET = 0;
  private static final int METADATA_RELOCATE_OFFSET = METADATA_ALIVE_SIZE_OFFSET + Constants.BYTES_IN_INT;
  public static final int METADATA_ALLOCATED_OFFSET = METADATA_RELOCATE_OFFSET + Constants.BYTES_IN_BYTE;
  public static final int METADATA_BYTES = 8;
  public static final int METADATA_PAGES_PER_REGION = (int) (EmbeddedMetaData.PAGES_IN_REGION / PAGES_IN_BLOCK * METADATA_BYTES / Constants.BYTES_IN_PAGE + 0.5f);
  // TODO: Incorrect calculation for PAGES_IN_BLOCK > 1
  public static final int BLOCKS_IN_REGION = (EmbeddedMetaData.PAGES_IN_REGION - METADATA_PAGES_PER_REGION) / PAGES_IN_BLOCK;
  public static final int BLOCKS_START_OFFSET = Constants.BYTES_IN_PAGE * METADATA_PAGES_PER_REGION;

  private static final Word PAGE_MASK = Word.fromIntZeroExtend(BYTES_IN_BLOCK - 1);

  // private static Address firstRegion = null;

  private static int count = 0;

  // Metadata setter

  private static void assertInMetadata(Address addr, int size) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(addr.GE(EmbeddedMetaData.getMetaDataBase(addr)));
      VM.assertions._assert(addr.plus(size).LE(EmbeddedMetaData.getMetaDataBase(addr).plus(METADATA_PAGES_PER_REGION * BYTES_IN_PAGE)));
    }
  }

  private static void set(Address addr, Address val) {
    assertInMetadata(addr, Constants.BYTES_IN_ADDRESS);
    addr.store(val);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadAddress().EQ(val));
  }

  private static void set(Address addr, int val) {
    assertInMetadata(addr, Constants.BYTES_IN_INT);
    addr.store(val);
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadInt() == val);
  }

  private static void set(Address addr, byte val) {
    assertInMetadata(addr, Constants.BYTES_IN_BYTE);
    addr.store(val);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadByte() == val);
  }

  // Block operations

  // private static Lock blockStateLock = VM.newLock("block-state-lock");
  // private static Lock blockRegistrationLock = VM.newLock("block-registration-lock");
  // public static Lock regionReleaseLock = VM.newLock("region-release-lock");

  public static Address of(final Address ptr) {
    return align(ptr);
  }

  public static boolean isAligned(final Address address) {
    return address.EQ(align(address));
  }

  public static boolean isValidBlock(final Address block) {
    int index = indexOf(block);
    return /*block != null &&*/ !block.isZero() && isAligned(block) && index >= 0 && index < BLOCKS_IN_REGION;
  }

  public static int count() {
    return count;
  }

  public static void setRelocationState(Address block, boolean relocation) {
    // blockStateLock.acquire();
    set(metaDataOf(block, METADATA_RELOCATE_OFFSET), (byte) (relocation ? 1 : 0));
    // blockStateLock.release();
  }

  public static boolean relocationRequired(Address block) {
    return metaDataOf(block, METADATA_RELOCATE_OFFSET).loadByte() != ((byte) 0);
  }

  public static void setUsedSize(Address block, int bytes) {
    //blockStateLock.acquire();
    set(metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET), bytes);
    //blockStateLock.release();
  }

  public static int usedSize(Address block) {
    return metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET).loadInt();
  }

  public static void register(Address block) {
    // blockRegistrationLock.acquire();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
    // Handle this block
    count += 1;
    clearState(block);
    setAllocated(block, true);
    // blockRegistrationLock.release();
  }

  public static void unregister(Address block) {
    // blockRegistrationLock.acquire();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
    count -= 1;
    clearState(block);
  }

  public static boolean allocated(Address block) {
    return metaDataOf(block, METADATA_ALLOCATED_OFFSET).loadByte() != ((byte) 0);
  }

  private static void clearState(Address block) {
    setAllocated(block, false);
    setRelocationState(block, false);
    setUsedSize(block, 0);
  }

  private static Address align(final Address ptr) {
    return ptr.toWord().and(PAGE_MASK.not()).toAddress();
  }

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

  private static Address metaDataOf(Address block, int medaDataOffset) {
    Address metaData = EmbeddedMetaData.getMetaDataBase(block);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(medaDataOffset >= 0 && medaDataOffset <= METADATA_BYTES);
    return metaData.plus(METADATA_OFFSET_IN_REGION + METADATA_BYTES * indexOf(block)).plus(medaDataOffset);
  }

  private static void setAllocated(Address block, boolean allocated) {
    //blockStateLock.acquire();
    set(metaDataOf(block, METADATA_ALLOCATED_OFFSET), (byte) (allocated ? 1 : 0));
    //blockStateLock.release();
  }
}
