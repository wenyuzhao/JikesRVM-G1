package org.mmtk.policy;

import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Word;

import java.util.Iterator;

import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class MarkRegion {
    public static final int PAGES_IN_REGION = 1;
    public static final int BYTES_IN_REGION = BYTES_IN_PAGE * PAGES_IN_REGION;

    private static final int PREV_POINTER_OFFSET_IN_MMTK_REGION = 0;
    private static final int NEXT_POINTER_OFFSET_IN_MMTK_REGION = PREV_POINTER_OFFSET_IN_MMTK_REGION + Constants.BYTES_IN_WORD;
    private static final int BLOCK_COUNT_OFFSET_IN_MMTK_REGION = NEXT_POINTER_OFFSET_IN_MMTK_REGION  + Constants.BYTES_IN_WORD;
    private static final int METADATA_OFFSET_IN_MMTK_REGION = BLOCK_COUNT_OFFSET_IN_MMTK_REGION + Constants.BYTES_IN_INT;


    private static final int METADATA_ALIVE_SIZE_OFFSET = 0;
    private static final int METADATA_RELOCATE_OFFSET = METADATA_ALIVE_SIZE_OFFSET + Constants.BYTES_IN_INT;
    private static final int METADATA_ALLOCATED_OFFSET = METADATA_RELOCATE_OFFSET + Constants.BYTES_IN_BYTE;
    private static final int METADATA_BYTES = 8;
    public static final int METADATA_PAGES_PER_MMTK_REGION = EmbeddedMetaData.PAGES_IN_REGION / PAGES_IN_REGION * METADATA_BYTES / Constants.BYTES_IN_PAGE;
    private static final int REGIONS_IN_MMTK_REGION = (EmbeddedMetaData.PAGES_IN_REGION - METADATA_PAGES_PER_MMTK_REGION) / PAGES_IN_REGION;
    private static final int REGIONS_START_OFFSET = Constants.BYTES_IN_PAGE * METADATA_PAGES_PER_MMTK_REGION;

    private static final Word PAGE_MASK = Word.fromIntZeroExtend(BYTES_IN_REGION - 1);

    static Address firstRegion = null;

    static {
        if (VM.VERIFY_ASSERTIONS) {
            Log.writeln("PAGES_IN_REGION ", EmbeddedMetaData.PAGES_IN_REGION);
            Log.writeln("BYTES_IN_PAGE ", Constants.BYTES_IN_PAGE);
            Log.writeln("METADATA_BYTES ", METADATA_BYTES);
            Log.writeln("BYTES_IN_REGION ", BYTES_IN_REGION);
            Log.writeln("METADATA_PAGES_PER_REGION ", METADATA_PAGES_PER_MMTK_REGION);
            Log.writeln("REGIONS_IN_MMTK_REGION ", REGIONS_IN_MMTK_REGION);
        }
    }



    public static Address of(final Address ptr) {
        return align(ptr);
    }

    public static boolean isAligned(final Address address) {
        return address.EQ(align(address));
    }

    public static boolean isValidBlock(final Address block) {
        return block != null && !block.isZero() && isAligned(block);
    }
    //private static Lock lock = VM.newLock("mark-block-register");
    public static void register(Address block) {
        //lock.acquire();
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
        count += 1;
        clearState(block);
        setAllocated(block, true);
        // Handle regions
        Address region = EmbeddedMetaData.getMetaDataBase(block);
        Address blockCount = region.plus(BLOCK_COUNT_OFFSET_IN_MMTK_REGION);
        int blocks = blockCount.loadInt() + 1;
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(blocks >= 1);
        if (blocks == 1) {
            // This is a new region
            if (firstRegion == null) {
                firstRegion = region;
                if (VM.VERIFY_ASSERTIONS) Log.writeln("Add First Region ", region);
            } else {
                region.plus(NEXT_POINTER_OFFSET_IN_MMTK_REGION).store(firstRegion);
                firstRegion.plus(PREV_POINTER_OFFSET_IN_MMTK_REGION).store(region);
                firstRegion = region;
                if (VM.VERIFY_ASSERTIONS) Log.writeln("Add Region ", region);
            }
            // Clear
        }
        blockCount.store(blocks);
        //lock.release();
    }

    public static void unregister(Address block) {
        //lock.acquire();
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
        count -= 1;
        clearState(block);
        // Handle regions
        Address region = EmbeddedMetaData.getMetaDataBase(block);
        Address blockCount = region.plus(BLOCK_COUNT_OFFSET_IN_MMTK_REGION);
        int blocks = blockCount.loadInt() - 1;
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(blocks >= 0);
        if (blocks == 0) {
            // This region shuold be removed
            if (VM.VERIFY_ASSERTIONS) Log.writeln("Remove Region ", region);
            if (region.EQ(firstRegion)) {
                Address next = region.plus(NEXT_POINTER_OFFSET_IN_MMTK_REGION).loadAddress();
                if (next.isZero()) {
                    firstRegion = null;
                } else {
                    firstRegion = next;
                    next.plus(PREV_POINTER_OFFSET_IN_MMTK_REGION).store(0);
                }
            } else {
                if (VM.VERIFY_ASSERTIONS) Log.writeln("Remove Region ", region);
                Address prev = region.plus(PREV_POINTER_OFFSET_IN_MMTK_REGION).loadAddress();
                Address next = region.plus(NEXT_POINTER_OFFSET_IN_MMTK_REGION).loadAddress();
                prev.plus(NEXT_POINTER_OFFSET_IN_MMTK_REGION).store(next);
                if (!next.isZero()) next.plus(PREV_POINTER_OFFSET_IN_MMTK_REGION).store(prev);
            }
            clearRegionState(region);
        } else {
            blockCount.store(blocks);
        }
        //lock.release();
    }

    //static Lock lock2 = VM.newLock("mark-region");
    public static void setRelocationState(Address block, boolean relocation) {
        if (relocation) {
            Log.writeln("@MarkRelocate: ", block);
        }
        //lock2.acquire();
        metaDataOf(block, METADATA_RELOCATE_OFFSET).store((byte) (relocation ? 1 : 0));
        //lock2.release();
    }

    public static boolean relocationRequired(Address block) {
        return metaDataOf(block, METADATA_RELOCATE_OFFSET).loadByte() > 0;
    }

    public static void setUsedSize(Address block, int bytes) {
        //lock2.acquire();
        metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET).store(bytes);
        //lock2.release();
    }

    public static int usedSize(Address block) {
        return metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET).loadInt();
    }

    public static int count() {
        return count;
    }



    private static Address align(final Address ptr) {
        return ptr.toWord().and(PAGE_MASK.not()).toAddress();
    }

    private static int indexOf(Address block) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero() && isAligned(block), "Invalid block");
        Address region = EmbeddedMetaData.getMetaDataBase(block);
        double index = block.diff(region.plus(REGIONS_START_OFFSET)).toInt() / BYTES_IN_REGION;
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(index == (int) index);
        return (int) index;
    }

    private static Address metaDataOf(Address block, int medaDataOffset) {
        Address metaData = EmbeddedMetaData.getMetaDataBase(block);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(medaDataOffset >= 0 && medaDataOffset <= METADATA_BYTES);
        return metaData.plus(METADATA_OFFSET_IN_MMTK_REGION + METADATA_BYTES * indexOf(block)).plus(medaDataOffset);
    }

    private static int count = 0;



    private static void setAllocated(Address block, boolean allocated) {
        //lock2.acquire();
        metaDataOf(block, METADATA_ALLOCATED_OFFSET).store((byte) (allocated ? 1 : 0));
        //lock2.release();
    }

    private static void clearRegionState(Address region) {
        //lock2.acquire();
        region.plus(PREV_POINTER_OFFSET_IN_MMTK_REGION).store((int) 0);
        region.plus(NEXT_POINTER_OFFSET_IN_MMTK_REGION).store((int) 0);
        region.plus(BLOCK_COUNT_OFFSET_IN_MMTK_REGION).store((int) 0);
        //lock2.release();
    }

    private static boolean allocated(Address block) {
        return metaDataOf(block, METADATA_ALLOCATED_OFFSET).loadByte() > 0;
    }

    private static void clearState(Address block) {
        setAllocated(block, false);
        setRelocationState(block, false);
        setUsedSize(block, 0);
    }

    // Iterator
    private static Address currentRegion = null;
    private static Address nextBlock = null;
    private static int curser = -1; // index of nextBlock in currentRegion

    public static void resetIterator() {
        //lock3.acquire();
        currentRegion = firstRegion;
        //lock3.release();
        moveToNextAllocatedBlock();
    }

    //private static Lock lock3 = VM.newLock("xxxxxx");
    private static void moveToNextAllocatedBlock() {
        //lock3.acquire();
        Log.write("#MMTK REGION ", currentRegion);
        Log.writeln(currentRegion == null || currentRegion.isZero() ? " true" : " false");
        if (currentRegion == null || currentRegion.isZero()) {
            currentRegion = null;
            nextBlock = null;
            curser = -1;
            //lock3.release();
            return;
        }
        for (int index = curser + 1; index < REGIONS_IN_MMTK_REGION; index++) {
            int offset = METADATA_OFFSET_IN_MMTK_REGION + index * METADATA_BYTES + METADATA_ALLOCATED_OFFSET;
            Address ptr = currentRegion.plus(offset);
            Log.write("# curser ", index);
            Log.writeln(" alloc_ptr ", ptr);
            if (ptr.loadByte() > 0) {
                curser = index;
                nextBlock = currentRegion.plus(REGIONS_START_OFFSET + BYTES_IN_REGION * index);
                Log.writeln("# allocated ", ptr.loadByte());
                //lock3.release();
                return;
            }
        }
        Address nextRegion = currentRegion.plus(NEXT_POINTER_OFFSET_IN_MMTK_REGION).loadAddress();
        Log.write("#REGION ", currentRegion);
        Log.write(" -> ", nextRegion);
        Log.writeln(" NEXT_POINTER_OFFSET_IN_MMTK_REGION ", NEXT_POINTER_OFFSET_IN_MMTK_REGION);
        currentRegion = nextRegion;
        curser = -1;
        //lock3.release();
        moveToNextAllocatedBlock();
    }

    public static boolean hasNext() {
        return nextBlock != null;
    }

    public static Address next() {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(nextBlock != null && !nextBlock.isZero());
        Address rtn = nextBlock;
        moveToNextAllocatedBlock();
        return rtn;
    }
}
