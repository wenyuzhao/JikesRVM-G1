package org.mmtk.policy;

import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.heap.layout.HeapLayout;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;
import org.vmmagic.unboxed.Word;

import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class MarkBlock {
    public static final int PAGES_IN_BLOCK = 1;
    public static final int BYTES_IN_BLOCK = BYTES_IN_PAGE * PAGES_IN_BLOCK;

    private static final int PREV_POINTER_OFFSET_IN_REGION = 0;
    private static final int NEXT_POINTER_OFFSET_IN_REGION = PREV_POINTER_OFFSET_IN_REGION + Constants.BYTES_IN_ADDRESS;
    private static final int BLOCK_COUNT_OFFSET_IN_REGION = NEXT_POINTER_OFFSET_IN_REGION + Constants.BYTES_IN_ADDRESS;
    private static final int METADATA_OFFSET_IN_REGION = BLOCK_COUNT_OFFSET_IN_REGION + Constants.BYTES_IN_INT;

    private static final int METADATA_ALIVE_SIZE_OFFSET = 0;
    private static final int METADATA_RELOCATE_OFFSET = METADATA_ALIVE_SIZE_OFFSET + Constants.BYTES_IN_INT;
    private static final int METADATA_ALLOCATED_OFFSET = METADATA_RELOCATE_OFFSET + Constants.BYTES_IN_BYTE;
    private static final int METADATA_BYTES = 8;
    public static final int METADATA_PAGES_PER_REGION = EmbeddedMetaData.PAGES_IN_REGION / PAGES_IN_BLOCK * METADATA_BYTES / Constants.BYTES_IN_PAGE;
    private static final int BLOCKS_IN_REGION = (EmbeddedMetaData.PAGES_IN_REGION - METADATA_PAGES_PER_REGION) / PAGES_IN_BLOCK;
    private static final int BLOCKS_START_OFFSET = Constants.BYTES_IN_PAGE * METADATA_PAGES_PER_REGION;

    private static final Word PAGE_MASK = Word.fromIntZeroExtend(BYTES_IN_BLOCK - 1);

    private static Address firstRegion = null;

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
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadInt() == val);
    }
    private static void set(Address addr, byte val) {
        assertInMetadata(addr, Constants.BYTES_IN_BYTE);
        addr.store(val);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(addr.loadByte() == val);
    }

    // Block operations

    private static Lock blockStateLock = VM.newLock("block-state-lock");
    private static Lock blockRegisterLock = VM.newLock("block-register-lock");

    public static Address of(final Address ptr) {
        return align(ptr);
    }

    public static boolean isAligned(final Address address) {
        return address.EQ(align(address));
    }

    public static boolean isValidBlock(final Address block) {
        int index = indexOf(block);
        return block != null && !block.isZero() && isAligned(block) && index >= 0 && index < BLOCKS_IN_REGION;
    }

    public static int count() {
        return count;
    }

    public static void setRelocationState(Address block, boolean relocation) {
        blockStateLock.acquire();
        set(metaDataOf(block, METADATA_RELOCATE_OFFSET), (byte) (relocation ? 1 : 0));
        blockStateLock.release();
    }

    public static boolean relocationRequired(Address block) {
        return metaDataOf(block, METADATA_RELOCATE_OFFSET).loadByte() != ((byte) 0);
    }

    public static void setUsedSize(Address block, int bytes) {
        blockStateLock.acquire();
        set(metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET), bytes);
        blockStateLock.release();
    }

    public static int usedSize(Address block) {
        return metaDataOf(block, METADATA_ALIVE_SIZE_OFFSET).loadInt();
    }

    public static void register(Address block) {
        blockRegisterLock.acquire();
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
        // Handle regions
        Address region = EmbeddedMetaData.getMetaDataBase(block);
        Address blockCount = region.plus(BLOCK_COUNT_OFFSET_IN_REGION);
        int blocks = blockCount.loadInt() + 1;
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(blocks >= 1);
        if (blocks == 1) {
            // This is a new region
            addRegion(region);
        }
        set(blockCount, blocks);
        // Handle this block
        count += 1;
        clearState(block);
        setAllocated(block, true);
        blockRegisterLock.release();
    }

    public static void unregister(Address block) {
        blockRegisterLock.acquire();
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isValidBlock(block));
        count -= 1;
        clearState(block);
        // Handle regions
        Address region = EmbeddedMetaData.getMetaDataBase(block);
        Address blockCount = region.plus(BLOCK_COUNT_OFFSET_IN_REGION);
        int blocks = blockCount.loadInt() - 1;
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(blocks >= 0);
        if (blocks == 0) {
            // This region should be removed
            removeRegion(region);
        } else {
            set(blockCount, blocks);
        }
        blockRegisterLock.release();
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

    private static int indexOf(Address block) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero() && isAligned(block), "Invalid block");
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
        blockStateLock.acquire();
        set(metaDataOf(block, METADATA_ALLOCATED_OFFSET), (byte) (allocated ? 1 : 0));
        blockStateLock.release();
    }

    // Region operations

    private static void addRegion(Address region) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(region.EQ(EmbeddedMetaData.getMetaDataBase(region)));
        clearRegionState(region);
        if (firstRegion == null || firstRegion.isZero()) {
            firstRegion = region;
            if (VM.DEBUG) Log.writeln("Add First Region ", region);
        } else {
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(firstRegion.plus(PREV_POINTER_OFFSET_IN_REGION).loadAddress().isZero());
            set(firstRegion.plus(PREV_POINTER_OFFSET_IN_REGION), region);
            set(region.plus(NEXT_POINTER_OFFSET_IN_REGION), firstRegion);
            firstRegion = region;
            if (VM.DEBUG) Log.writeln("Add Region ", region);
        }
    }

    private static void removeRegion(Address region) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(region.EQ(EmbeddedMetaData.getMetaDataBase(region)));
        if (VM.DEBUG) Log.writeln("Remove Region ", region);
        if (region.EQ(firstRegion)) {
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(region.plus(PREV_POINTER_OFFSET_IN_REGION).loadAddress().isZero());
            Address next = region.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress();
            if (next.isZero()) {
                firstRegion = null;
            } else {
                if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(next.plus(PREV_POINTER_OFFSET_IN_REGION).loadAddress().EQ(region));
                firstRegion = next;
                set(next.plus(PREV_POINTER_OFFSET_IN_REGION), Address.zero());
            }
        } else {
            Address prev = region.plus(PREV_POINTER_OFFSET_IN_REGION).loadAddress();
            Address next = region.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress();
            if (VM.VERIFY_ASSERTIONS) {
                VM.assertions._assert(prev.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress().EQ(region));
                if (!next.isZero()) VM.assertions._assert(next.plus(PREV_POINTER_OFFSET_IN_REGION).loadAddress().EQ(region));
            }
            set(prev.plus(NEXT_POINTER_OFFSET_IN_REGION), next);
            if (!next.isZero()) set(next.plus(PREV_POINTER_OFFSET_IN_REGION), prev);
        }
        clearRegionState(region);
    }

    private static void clearRegionState(Address region) {
        blockStateLock.acquire();
        set(region.plus(PREV_POINTER_OFFSET_IN_REGION), Address.zero());
        set(region.plus(NEXT_POINTER_OFFSET_IN_REGION), Address.zero());
        set(region.plus(BLOCK_COUNT_OFFSET_IN_REGION), (int) 0);
        blockStateLock.release();
    }

    public static void clearRegionMetadata(Address chunk) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(chunk.EQ(EmbeddedMetaData.getMetaDataBase(chunk)));
        HeapLayout.mmapper.ensureMapped(chunk, METADATA_PAGES_PER_REGION);
        VM.memory.zero(false, chunk, Extent.fromIntZeroExtend(BLOCKS_START_OFFSET));
    }

    // Block iterator

    public static Address firstBlock() {
        return nextBlock(firstRegion);
    }

    public static Address nextBlock(Address block) {
        if (VM.VERIFY_ASSERTIONS) {
            if (!(block.EQ(EmbeddedMetaData.getMetaDataBase(block)) || (indexOf(block) >= 0 && indexOf(block) < BLOCKS_IN_REGION))) {
                Log.write("Invalid block ", block);
                Log.write(" at region ", EmbeddedMetaData.getMetaDataBase(block));
                Log.write(" with index ", indexOf(block));
                Log.writeln(isAligned(block) ? " aligned" : " not aligned");
            }
            VM.assertions._assert(block.EQ(EmbeddedMetaData.getMetaDataBase(block)) || (indexOf(block) >= 0 && indexOf(block) < BLOCKS_IN_REGION));

        }
        int i = block.EQ(EmbeddedMetaData.getMetaDataBase(block)) ? 0 : indexOf(block) + 1;
        Address region = EmbeddedMetaData.getMetaDataBase(block);
        if (i >= BLOCKS_IN_REGION) {
            i = 0;
            region = region.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress();
        }

        while (true) {
            if (region.isZero()) return null;

            Address allocated = region.plus(METADATA_OFFSET_IN_REGION + i * METADATA_BYTES + METADATA_ALLOCATED_OFFSET);
            if (allocated.loadByte() != ((byte) 0)) {
                Address rtn = region.plus(BLOCKS_START_OFFSET + i * BYTES_IN_BLOCK);
                return rtn;
            }

            i++;
            if (i >= BLOCKS_IN_REGION) {
                i = 0;
                region = region.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress();
            }
        }
    }
}
