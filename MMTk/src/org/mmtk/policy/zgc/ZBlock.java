package org.mmtk.policy.zgc;

import org.mmtk.utility.Conversions;
import org.mmtk.vm.VM;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;
import org.vmmagic.unboxed.Word;

import static org.mmtk.utility.Constants.*;

public class ZBlock {
    public static final int LOG_BYTES = 15;
    public static final int BYTES = 1 << 15;
    public static final Word BLOCK_MASK = Word.fromIntZeroExtend(BYTES - 1);
    static final int LOG_PAGES = LOG_BYTES - LOG_BYTES_IN_PAGE;
    static final int PAGES = 1 << LOG_PAGES;

    static final int METADATA_BYTES = 8;
    //static final int ROUNDED_METADATA_PAGES_PER_CHUNK = ROUNDED_METADATA_BYTES_PER_CHUNK >> LOG_BYTES_IN_PAGE;
    //public static final int FIRST_USABLE_BLOCK_INDEX = ROUNDED_METADATA_BYTES_PER_CHUNK >> LOG_BYTES_IN_BLOCK;

    static final short UNALLOCATED = 0;
    static final short IN_USE = 1;

    public static Address align(final Address ptr) {
        return ptr.toWord().and(BLOCK_MASK.not()).toAddress();
    }

    public static boolean isAligned(final Address address) {
        return address.EQ(align(address));
    }

    public static void setBlockState(Address address, short value) {
        getBlockMarkStateAddress(address).store(value);
    }

    /**
     * @return the number of pages of metadata required per chunk
     */
    static int getRequiredMetaDataPages() {
        Extent bytes = Extent.fromIntZeroExtend(METADATA_BYTES);
        return Conversions.bytesToPagesUp(bytes);
    }

    static Address getBlockMarkStateAddress(Address address) {
        return address;
        /*Address chunk = Chunk.align(address);
        int index = getChunkIndex(address);
        Address rtn = chunk.plus(Chunk.BLOCK_STATE_TABLE_OFFSET).plus(index << LOG_BYTES_IN_BLOCK_STATE_ENTRY);
        if (VM.VERIFY_ASSERTIONS) {
            Address block = chunk.plus(index << ZBlock.LOG_BYTES);
            VM.assertions._assert(isAligned(block));
            boolean valid = rtn.GE(chunk.plus(Chunk.BLOCK_STATE_TABLE_OFFSET)) && rtn.LT(chunk.plus(Chunk.BLOCK_STATE_TABLE_OFFSET + BLOCK_STATE_TABLE_BYTES));
            VM.assertions._assert(valid);
        }
        return rtn;
        */
    }
}
