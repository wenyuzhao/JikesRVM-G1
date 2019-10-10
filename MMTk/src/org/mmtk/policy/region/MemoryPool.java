package org.mmtk.policy.region;

import org.mmtk.plan.Plan;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;

import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class MemoryPool {
    private static final Offset NEXT_OFFSET = Offset.fromIntZeroExtend(0);
    private static final Offset HEAD_OFFSET = VM.objectModel.getFieldOffset(MemoryPool.class, "head", Address.class);
    private static final Offset CURSOR_OFFSET = VM.objectModel.getFieldOffset(MemoryPool.class, "cursor", Address.class);
    private static final int PAGES_PER_SLOW_ALLOCATION = 256;
    private static final int BYTES_PER_SLOW_ALLOCATION = PAGES_PER_SLOW_ALLOCATION * BYTES_IN_PAGE;

    private final Lock lock = VM.newLock("memory-pool-lock");
    private final int CELL_SIZE;
    @Entrypoint private Address head = Address.zero();
    private int popCount = 0;
    @Entrypoint private Address cursor = Address.zero();
    private Address limit = Address.zero();

    public MemoryPool(int unitSize) {
        CELL_SIZE = unitSize;
    }

    @Inline
    private boolean doubleCAS(Address oldAddress, Address newAddress, int oldCount, int newCount) {
        long oldValue = oldAddress.toLong() | (((long) oldCount) << BITS_IN_INT);
        long newValue = newAddress.toLong() | (((long) newCount) << BITS_IN_INT);
        return VM.memory.attemptLong(this, HEAD_OFFSET, oldValue, newValue);
    }

    @Inline
    private Address zero(Address cell) {
        VM.memory.zero(false, cell, Extent.fromIntZeroExtend(CELL_SIZE));
        return cell;
    }

    @Inline
    private Address popNode() {
        Address headSlot = ObjectReference.fromObject(this).toAddress().plus(HEAD_OFFSET);
        Address oldHead, newHead;
        do {
            oldHead = headSlot.loadAddress();
            if (oldHead.isZero()) return oldHead;
            newHead = oldHead.loadAddress(NEXT_OFFSET);
        } while (!doubleCAS(oldHead, newHead, popCount, popCount + 1));
        return zero(oldHead);
    }

    @Inline
    private void pushNode(Address node) {
        Address headSlot = ObjectReference.fromObject(this).toAddress().plus(HEAD_OFFSET);
        Address oldHead;
        final Address newHead = node;
        do {
            oldHead = headSlot.loadAddress();
            newHead.store(oldHead);
        } while (!headSlot.attempt(oldHead, newHead));
    }

    @Inline
    public Address alloc() {
        Address node = allocFast();
        if (!node.isZero()) {
            return node;
        }
        return allocSlow();
    }

    @Inline
    private Address tryBumpAlloc() {
        if (cursor.isZero()) return Address.zero();
        final int cellSize = this.CELL_SIZE;
        Address cursorSlot = ObjectReference.fromObject(this).toAddress().plus(CURSOR_OFFSET);
        Address oldCursor, newCursor;
        do {
            oldCursor = cursorSlot.prepareAddress();
            if (oldCursor.isZero()) return Address.zero();
            newCursor = oldCursor.plus(cellSize);
            if (newCursor.GT(limit)) {
                cursor = Address.zero();
                limit = Address.zero();
                return Address.zero();
            }
        } while (!cursorSlot.attempt(oldCursor, newCursor));
        return oldCursor;
    }

    @Inline
    private Address allocFast() {
        Address cell = tryBumpAlloc();
        if (!cell.isZero()) return cell;
        return popNode();
    }

    @NoInline
    private Address allocSlow() {
        lock.acquire();
        {
            Address node = allocFast();
            if (!node.isZero()) {
                lock.release();
                return node;
            }
        }

        final Address chunk = Plan.metaDataSpace.acquire(PAGES_PER_SLOW_ALLOCATION);
        if (chunk.isZero()) VM.assertions.fail("OutOfMemory");
        cursor = chunk.plus(CELL_SIZE);
        limit = chunk.plus(BYTES_PER_SLOW_ALLOCATION);
        lock.release();
        return zero(chunk);
    }

    @Inline
    public void free(Address cell) {
        pushNode(cell);
    }
}
