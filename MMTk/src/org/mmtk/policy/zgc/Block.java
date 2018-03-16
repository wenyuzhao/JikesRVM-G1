package org.mmtk.policy.zgc;

import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.heap.layout.HeapLayout;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Word;

import java.util.Iterator;

import static org.mmtk.utility.Constants.*;

public class Block {
    public static final int BYTES_IN_BLOCK = BYTES_IN_PAGE;
    public static final int PAGES_IN_BLOCK = 1;

    public static final int PREV_POINTER_OFFSET_IN_REGION = 0;
    public static final int NEXT_POINTER_OFFSET_IN_REGION = 4;
    public static final int BLOCK_COUNT_OFFSET_IN_REGION = 8;
    public static final int METADATA_OFFSET_IN_REGION = 16;

    public static final int METADATA_BYTES = 8;
    public static final int METADATA_ALLOCATED_OFFSET = 5;
    public static final int METADATA_RELOCATE_OFFSET = 4;
    public static final int METADATA_ALIVE_SIZE_OFFSET = 0;
    public static final int METADATA_PAGES_PER_REGION = EmbeddedMetaData.PAGES_IN_REGION / PAGES_IN_BLOCK * METADATA_BYTES / Constants.BYTES_IN_PAGE;
    public static final int BLOCKS_IN_REGION = (EmbeddedMetaData.PAGES_IN_REGION - METADATA_PAGES_PER_REGION) / PAGES_IN_BLOCK;
    public static final int BLOCKS_START_OFFSET = Constants.BYTES_IN_PAGE * METADATA_PAGES_PER_REGION;

    public static final Word PAGE_MASK = Word.fromIntZeroExtend(BYTES_IN_BLOCK - 1);

    static {

        if (VM.VERIFY_ASSERTIONS) {
            Log.writeln("PAGES_IN_REGION " + EmbeddedMetaData.PAGES_IN_REGION);
            Log.writeln("BYTES_IN_PAGE " + Constants.BYTES_IN_PAGE);
            Log.writeln("METADATA_BYTES " + METADATA_BYTES);
            Log.writeln("BYTES_IN_BLOCK " + BYTES_IN_BLOCK);
            Log.writeln("METADATA_PAGES_PER_REGION " + METADATA_PAGES_PER_REGION);
            Log.writeln("BLOCKS_IN_REGION " + BLOCKS_IN_REGION);
        }
    }

    public static Address of(final Address ptr) {
        return align(ptr);
    }

    public static Address align(final Address ptr) {
        return ptr.toWord().and(PAGE_MASK.not()).toAddress();
    }

    public static boolean isAligned(final Address address) {
        return address.EQ(align(address));
    }

    public static boolean isValidBlock(final Address block) {
        return block != null && !block.isZero() && isAligned(block);
    }

    private static int indexOf(Address block) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero() && isAligned(block), "Invalid block " + block);
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

    public static void setRelocationState(Address block, boolean relocation) {
        metaDataOf(block, METADATA_RELOCATE_OFFSET).store((byte) (relocation ? 1 : 0));
    }

    public static boolean relocationRequired(Address block) {
        return metaDataOf(block, METADATA_RELOCATE_OFFSET).loadByte() > 0;
    }

    public static synchronized void setUsedSize(Address block, int bytes) {
        metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET).store(bytes);
    }

    public static int usedSize(Address block) {
        return metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET).loadInt();
    }

    private static int count = 0;

    public static synchronized void setAllocated(Address block, boolean allocated) {
        if (allocated(block) == allocated) return;
        metaDataOf(block, METADATA_ALLOCATED_OFFSET).store((byte) (allocated ? 1 : 0));
        Address region = EmbeddedMetaData.getMetaDataBase(block);
        Address blockCount = region.plus(BLOCK_COUNT_OFFSET_IN_REGION);
        if (allocated) {
            count += 1;
            int oldBlocks = blockCount.loadInt();
            if (oldBlocks <= 0) {
                if (firstRegion == null) {
                    firstRegion = region;
                    Log.writeln("Add First Region " + region);
                } else {
                    region.plus(NEXT_POINTER_OFFSET_IN_REGION).store(firstRegion);
                    firstRegion.plus(PREV_POINTER_OFFSET_IN_REGION).store(region);
                    firstRegion = region;
                    Log.writeln("Add Region " + region);
                }
            }
            blockCount.store(oldBlocks + 1);
        } else {
            count -= 1;
            int blocks = blockCount.loadInt() - 1;
            if (blocks <= 0) {
                if (region.EQ(firstRegion)) {
                    firstRegion = null;
                    Log.writeln("Remove Last Region " + region);
                } else {
                    Log.writeln("Remove Region " + region);
                    Address prev = region.plus(PREV_POINTER_OFFSET_IN_REGION).loadAddress();
                    Address next = region.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress();
                    prev.plus(NEXT_POINTER_OFFSET_IN_REGION).store(next);
                    if (!next.isZero()) next.plus(PREV_POINTER_OFFSET_IN_REGION).store(prev);
                }
            }
            blockCount.store(blocks);
        }
    }

    public static boolean allocated(Address block) {
        return metaDataOf(block, METADATA_ALLOCATED_OFFSET).loadByte() > 0;
    }

    public static synchronized void clearState(Address block) {
        setAllocated(block, false);
        setRelocationState(block, false);
        setUsedSize(block, 0);
    }

    public static int count() {
        return count;
    }

    static Address firstRegion = null;


    static class BlocksIterator implements Iterator<Address> {
        Address currentRegion = null;
        Address nextBlock = null;
        int curser = -1; // index of nextBlock in currentRegion

        BlocksIterator(Address firstRegion) {
            currentRegion = firstRegion;
            moveToNextAllocatedBlock();
        }

        void moveToNextAllocatedBlock() {
            if (currentRegion == null || currentRegion.isZero()) {
                currentRegion = null;
                nextBlock = null;
                curser = -1;
                return;
            }
            for (int index = curser + 1; index < BLOCKS_IN_REGION; index++) {
                if (currentRegion.plus(METADATA_OFFSET_IN_REGION + index * METADATA_BYTES + METADATA_ALLOCATED_OFFSET).loadByte() > 0) {
                    curser = index;
                    nextBlock = currentRegion.plus(BLOCKS_START_OFFSET + BYTES_IN_BLOCK * index);
                    return;
                }
            }
            currentRegion = currentRegion.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress();
            curser = -1;
            moveToNextAllocatedBlock();
        }

        @Override
        public boolean hasNext() {
            return nextBlock != null;
        }

        @Override
        public Address next() {
            Address rtn = nextBlock;
            moveToNextAllocatedBlock();
            return rtn;
        }
    }

    public static Iterable<Address> iterate() {
        return new Iterable<Address>() {
            @Override
            public Iterator<Address> iterator() {
                return new BlocksIterator(firstRegion);
            }
        };
    }
}
