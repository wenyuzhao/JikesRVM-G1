package org.mmtk.plan.g1;

import org.mmtk.plan.Plan;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Conversions;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Entrypoint;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;

import static org.mmtk.utility.Constants.BITS_IN_INT;
import static org.mmtk.utility.Constants.BYTES_IN_PAGE;

@Uninterruptible
public class BufferQueue {
    private static final int METADATA_BYTES = Constants.BYTES_IN_ADDRESS;
    public static final int LOCAL_BUFFER_SIZE = 4096 - METADATA_BYTES;
    private static final int PAGES_IN_BUFFER = ((LOCAL_BUFFER_SIZE + Constants.BYTES_IN_PAGE - 1) / Constants.BYTES_IN_PAGE);

    private static final Offset NEXT_OFFSET = Offset.fromIntZeroExtend(0);

    private static final Offset HEAD_OFFSET = VM.objectModel.getFieldOffset(BufferQueue.class, "head", Address.class);

    @Entrypoint private Address head = Address.zero();
    private int popCount = 0;
    private Atomic.Int size = new Atomic.Int();

    @Inline
    private boolean doubleCAS(Address oldAddress, Address newAddress, int oldCount, int newCount) {
        long oldValue = oldAddress.toLong() | (((long) oldCount) << BITS_IN_INT);
        long newValue = newAddress.toLong() | (((long) newCount) << BITS_IN_INT);
        return VM.memory.attemptLong(this, HEAD_OFFSET, oldValue, newValue);
    }

    @Inline
    private Address dequeueInternal() {
        Address headSlot = ObjectReference.fromObject(this).toAddress().plus(HEAD_OFFSET);
        Address oldHead, newHead;
        do {
            oldHead = headSlot.loadAddress();
            if (oldHead.isZero()) return oldHead;
            newHead = oldHead.loadAddress(NEXT_OFFSET);
        } while (!doubleCAS(oldHead, newHead, popCount, popCount + 1));
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Conversions.isPageAligned(oldHead));
        size.add(-1);
        return oldHead;
    }

    @Inline
    private void enqueueInternal(Address node) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Conversions.isPageAligned(node));
        Address headSlot = ObjectReference.fromObject(this).toAddress().plus(HEAD_OFFSET);
        Address oldHead;
        final Address newHead = node;
        do {
            oldHead = headSlot.loadAddress();
            newHead.store(oldHead, NEXT_OFFSET);
        } while (!headSlot.attempt(oldHead, newHead));
        size.add(1);
    }

    @Inline
    public Address allocateLocalQueue() {
        Address a = G1.metaDataSpace.acquire(PAGES_IN_BUFFER);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!a.isZero());
        return a.plus(METADATA_BYTES);
    }

    @Inline
    public void release(Address buffer) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!buffer.isZero());
        G1.metaDataSpace.release(Conversions.pageAlign(buffer));
    }

    @Inline
    public void enqueue(Address buffer) {
        Address node = Conversions.pageAlign(buffer);
        enqueueInternal(node);
        if (G1.ENABLE_CONCURRENT_REFINEMENT) {
            if (size.get() > 5 && !ConcurrentRefinementWorker.forceIdle) {
                ConcurrentRefinementWorker.GROUP.triggerCycle();
            }
        }
    }

    @Inline
    public Address dequeue() {
        Address node = dequeueInternal();
        if (node.isZero()) return node;
        return node.plus(METADATA_BYTES);
    }
}
