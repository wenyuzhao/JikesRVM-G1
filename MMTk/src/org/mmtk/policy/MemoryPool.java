package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.utility.Conversions;
import org.mmtk.utility.Log;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;
import org.vmmagic.unboxed.Offset;

import static org.mmtk.utility.Constants.*;
import static org.mmtk.utility.Constants.BYTES_IN_PAGE;

// Unit: [ next (4b), prev (4b), data... ]

@Uninterruptible
public class MemoryPool {
    private final Lock lock = VM.newLock("MemPoolLock");
    public  final int BYTES_IN_UNIT;
    private final int UNITS_IN_PAGE;
    private Address list = Address.zero();
    private final Offset NEXT_SLOT = Offset.fromIntZeroExtend(0);
    private final Offset PREV_SLOT = Offset.fromIntZeroExtend(BYTES_IN_ADDRESS);
    private int pages = 0;

    public MemoryPool(int bytesInUnit) {
        BYTES_IN_UNIT = bytesInUnit;
        UNITS_IN_PAGE = BYTES_IN_PAGE / BYTES_IN_UNIT - 1;
    }

    static Address deadbeaf(Address a, int bytes) {
        for (int i = 0; i < bytes; i += 4) {
            a.store(0xdeadbeaf, Offset.fromIntZeroExtend(i));
        }
        return a;
    }

    public int pages() {
        return pages;
    }

    public Address alloc() {
        lock.acquire();
        if (list.isZero()) {
            Address cursor = Plan.metaDataSpace.acquire(1);
            pages += 1;
            Address limit = cursor.plus(BYTES_IN_PAGE);
            cursor = cursor.plus(BYTES_IN_UNIT);
            list = cursor;
            while (cursor.LT(limit)) {
                Address next = cursor.plus(BYTES_IN_UNIT);
                if (next.LT(limit)) {
                    if (VM.VERIFY_ASSERTIONS) {
                        if (!Space.isInSpace(Plan.metaDataSpace.getDescriptor(), list)) {
                            Log.writeln("Invalid address ", list);
                        }
                        VM.assertions._assert(Space.isInSpace(Plan.metaDataSpace.getDescriptor(), list));
                    }
                    cursor.store(next, NEXT_SLOT);
                    next.store(cursor, PREV_SLOT);
                }
                cursor = next;
            }
        }
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!list.isZero());
        Address rtn = list;
        // Increment live-remset count
        Address page = Conversions.pageAlign(rtn);
        page.store(page.loadInt() + 1);

        // Update freelist
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!list.isZero());
        list = list.loadAddress(NEXT_SLOT);
//        Log.writeln("next cell ", list);
        if (!list.isZero()) {
            if (VM.VERIFY_ASSERTIONS) {
                if (!Space.isInSpace(Plan.metaDataSpace.getDescriptor(), list)) {
                    Log.write("Invalid address ", list);
                    Log.writeln(Space.isMappedAddress(list) ? " mapped " : " unmapped ");
                    if (Space.isMappedAddress(list)) {
                        Log.write("Space: ");
                        Log.writeln(Space.getSpaceForAddress(list).getName());
                    }
                }
                VM.assertions._assert(Space.isInSpace(Plan.metaDataSpace.getDescriptor(), list));
            }

//          Log.writeln("PREV_SLOT: ", PREV_SLOT);
//          Log.writeln("BYTES_IN_UNIT: ", BYTES_IN_UNIT);
            list.store(Address.zero(), PREV_SLOT);
        }
        // Zero memory
        VM.memory.zero(false, rtn, Extent.fromIntZeroExtend(BYTES_IN_UNIT));
        lock.release();
        return rtn;
    }

    public void free(Address block) {
        lock.acquire();
        if (VM.VERIFY_ASSERTIONS) {
            VM.assertions._assert(!block.isZero());
            VM.assertions._assert(Space.isInSpace(Plan.metaDataSpace.getDescriptor(), block));
        }
//        Log.writeln("alloc cell ", block);
        // Add block to freelist
//        if (VM.VERIFY_ASSERTIONS) deadbeaf(block, BYTES_IN_UNIT);
        block.store(list, NEXT_SLOT);
        block.store(Address.zero(), PREV_SLOT);
        if (!list.isZero())
            list.store(block, PREV_SLOT);
        list = block;
        // Decrement live-remset count
        final Address page = Conversions.pageAlign(block);
        int count = page.loadInt();
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(count >= 1 && count <= UNITS_IN_PAGE);
        if (count > 1) {
            page.store(count - 1);
        } else {
            // Free this page
            // 1. Remove all cells from freelist
            while (Conversions.pageAlign(list).EQ(page)) {
                list = list.loadAddress(NEXT_SLOT);
                if (!list.isZero())
                    list.store(Address.zero(), PREV_SLOT);
            }
            Address cursor = page.plus(BYTES_IN_UNIT), limit = page.plus(BYTES_IN_PAGE);
            while (cursor.LT(limit)) {
                Address prev = cursor.loadAddress(PREV_SLOT);
                Address next = cursor.loadAddress(NEXT_SLOT);
                if (!prev.isZero()) {
                    prev.store(next, NEXT_SLOT);
                }
                if (!next.isZero()) {
                    next.store(prev, PREV_SLOT);
                }
                cursor = cursor.plus(BYTES_IN_UNIT);
            }
            // 2. Release page
            Plan.metaDataSpace.release(page);
            pages -= 1;
        }
        lock.release();
    }
}
