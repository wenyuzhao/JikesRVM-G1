/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */
package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.utility.*;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.heap.*;

import org.mmtk.vm.VM;

import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.*;

/**
 * Each instance of this class corresponds to one immix <b>space</b>.
 * Each of the instance methods of this class may be called by any
 * thread (i.e. synchronization must be explicit in any instance or
 * class method).  This contrasts with the SquishLocal, where
 * instances correspond to *plan* instances and therefore to kernel
 * threads.  Thus unlike this class, synchronization is not necessary
 * in the instance methods of SquishLocal.
 *
 */
@Uninterruptible
public final class MarkRegionSpace extends Space {
    /** number of header bits we may use */
    private static final int AVAILABLE_LOCAL_BITS = 8 - HeaderByte.USED_GLOBAL_BITS;

    /* local status bits */
    private static final byte NEW_OBJECT_MARK = 0; // using zero means no need for explicit initialization on allocation
    private static final int MARK_BASE = ForwardingWord.FORWARDING_BITS;
    private static final int  MAX_MARKCOUNT_BITS = AVAILABLE_LOCAL_BITS - MARK_BASE;
    private static final byte MARK_INCREMENT = 1 << MARK_BASE;
    private static final byte MARK_MASK = (byte) (((1 << MAX_MARKCOUNT_BITS) - 1) << MARK_BASE);
    private static final byte MARK_AND_FORWARDING_MASK = (byte) (MARK_MASK | ForwardingWord.FORWARDING_MASK);
    private static final byte MARK_BASE_VALUE = MARK_INCREMENT;

    public static final int LOCAL_GC_BITS_REQUIRED = AVAILABLE_LOCAL_BITS;
    public static final int GLOBAL_GC_BITS_REQUIRED = 0;
    public static final int GC_HEADER_WORDS_REQUIRED = 0;

    static class Header {
        static byte markState = MARK_BASE_VALUE;
        static boolean isNewObject(ObjectReference object) {
            return (VM.objectModel.readAvailableByte(object) & MARK_AND_FORWARDING_MASK) == NEW_OBJECT_MARK;
        }
        static boolean isMarked(ObjectReference object) {
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((markState & MARK_MASK) == markState);
            return (VM.objectModel.readAvailableByte(object) & MARK_MASK) == markState;
        }
        static boolean isMarked(byte gcByte) {
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((markState & MARK_MASK) == markState);
            return (gcByte & MARK_MASK) == markState;
        }
        static boolean testAndMark(ObjectReference object) {
            byte oldValue, newValue, oldMarkState;

            oldValue = VM.objectModel.readAvailableByte(object);
            oldMarkState = (byte) (oldValue & MARK_MASK);
            if (oldMarkState != markState) {
                newValue = (byte) ((oldValue & ~MARK_MASK) | markState);
                if (HeaderByte.NEEDS_UNLOGGED_BIT)
                    newValue |= HeaderByte.UNLOGGED_BIT;
                VM.objectModel.writeAvailableByte(object, newValue);
            }
            return oldMarkState != markState;
        }
        static void deltaMarkState(boolean increment) {
            byte rtn = markState;
            do {
                rtn = (byte) (increment ? rtn + MARK_INCREMENT : rtn - MARK_INCREMENT);
                rtn &= MARK_MASK;
            } while (rtn < MARK_BASE_VALUE);
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(rtn != markState);
            markState = rtn;
        }
        static void writeMarkState(ObjectReference object) {
            byte oldValue = VM.objectModel.readAvailableByte(object);
            byte markValue = markState;
            byte newValue = (byte) (oldValue & ~MARK_AND_FORWARDING_MASK);
            if (HeaderByte.NEEDS_UNLOGGED_BIT)
                newValue |= HeaderByte.UNLOGGED_BIT;
            newValue |= markValue;
            VM.objectModel.writeAvailableByte(object, newValue);
        }
        static void returnToPriorStateAndEnsureUnlogged(ObjectReference object, byte status) {
            if (HeaderByte.NEEDS_UNLOGGED_BIT) status |= HeaderByte.UNLOGGED_BIT;
            VM.objectModel.writeAvailableByte(object, status);
        }
        static void setMarkStateUnlogAndUnlock(ObjectReference object, byte gcByte) {
            byte oldGCByte = gcByte;
            byte newGCByte = (byte) ((oldGCByte & ~MARK_AND_FORWARDING_MASK) | markState);
            if (HeaderByte.NEEDS_UNLOGGED_BIT) newGCByte |= HeaderByte.UNLOGGED_BIT;
            VM.objectModel.writeAvailableByte(object, newGCByte);
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((oldGCByte & MARK_MASK) != markState);
        }
    }

    /**
     * The caller specifies the region of virtual memory to be used for
     * this space.  If this region conflicts with an existing space,
     * then the constructor will fail.
     *
     * @param name The name of this space (used when printing error messages etc)
     * @param vmRequest The virtual memory request
     */
    public MarkRegionSpace(String name, VMRequest vmRequest) {
        this(name, true, vmRequest);
    }

    /**
     * The caller specifies the region of virtual memory to be used for
     * this space.  If this region conflicts with an existing space,
     * then the constructor will fail.
     *
     * @param name The name of this space (used when printing error messages etc)
     * @param zeroed if true, allocations return zeroed memory
     * @param vmRequest The virtual memory request
     */
    public MarkRegionSpace(String name, boolean zeroed, VMRequest vmRequest) {
        super(name, false, false, zeroed, vmRequest);
        if (vmRequest.isDiscontiguous())
            pr = new FreeListPageResource(this, MarkRegion.METADATA_PAGES_PER_MMTK_REGION);
        else
            pr = new FreeListPageResource(this, start, extent, MarkRegion.METADATA_PAGES_PER_MMTK_REGION);
    }

    /**
     * Prepare for a new collection increment.
     */
    public void prepare() {
        Header.deltaMarkState(true);
    }

    /**
     * A new collection increment has completed.  Release global resources.
     */
    public void release() {
    }

    /**
     * Return the number of pages reserved for copying.
     */
    public int getCollectionReserve() {
        return 0;
    }

    /**
     * Return the number of pages reserved for use given the pending
     * allocation.  This is <i>exclusive of</i> space reserved for
     * copying.
     */
    public int getPagesUsed() {
        return pr.reservedPages() - getCollectionReserve();
    }

    /**
     * Return a pointer to a set of new usable blocks, or null if none are available.
     * Use different block selection heuristics depending on whether the allocation
     * request is "hot" or "cold".
     *
     * @param copy Whether the space is for relocation
     * @return the pointer into the alloc table containing usable blocks, {@code null}
     *  if no usable blocks are available
     */
    public Address getSpace(boolean copy) {
        // Allocate
        Address region = acquire(MarkRegion.PAGES_IN_REGION);

        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkRegion.isAligned(region));

        if (!region.isZero()) {
            VM.memory.zero(false, region, Extent.fromIntZeroExtend(MarkRegion.BYTES_IN_REGION));;
            Log.writeln("#Block alloc " + region + ", in region " + EmbeddedMetaData.getMetaDataBase(region));
            MarkRegion.register(region);
        }
        return region;
    }

    /**
     * Release a block.  A block is free, so call the underlying page allocator
     * to release the associated storage.
     *
     * @param region The address of the Z Page to be released
     */
    @Override
    @Inline
    public void release(Address region) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkRegion.isAligned(region));
        MarkRegion.unregister(region);
        ((FreeListPageResource) pr).releasePages(region);
    }

    /**
     * Perform any required post allocation initialization
     *
     * @param object the object ref to the storage to be initialized
     * @param bytes size of the allocated object in bytes
     */
    @Inline
    public void postAlloc(ObjectReference object, int bytes) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Header.isNewObject(object));
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
    }

    /**
     * Perform any required post copy (i.e. in-GC allocation) initialization.
     * This is relevant (for example) when Squish is used as the mature space in
     * a copying GC.
     *
     * @param object the object ref to the storage to be initialized
     * @param bytes size of the copied object in bytes
     */
    @Inline
    public void postCopy(ObjectReference object, int bytes) {
        Header.writeMarkState(object);
        ForwardingWord.clearForwardingBits(object);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
        if (VM.VERIFY_ASSERTIONS && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(object));
    }

    /**
     * Trace a reference to an object.  This interface is not supported by immix, since
     * we require the allocator to be identified except for the special case of the fast
     * trace.
     *
     * @param trace The trace performing the transitive closure
     * @param object The object to be traced.
     * @return null and fail.
     */
    @Override
    public ObjectReference traceObject(TransitiveClosure trace, ObjectReference object) {
        VM.assertions.fail("unsupported interface");
        return null;
    }

    /**
     * Trace an object under a copying collection policy.
     * If the object is already copied, the copy is returned.
     * Otherwise, a copy is created and returned.
     * In either case, the object will be marked on return.
     *
     * @param trace The trace being conducted.
     * @param object The object to be forwarded.
     * @return The forwarded object.
     */
    @Inline
    public ObjectReference traceMarkObject(TraceLocal trace, ObjectReference object) {
        ObjectReference rtn = object;
        if (ForwardingWord.isForwarded(object)) {
            rtn = getForwardingPointer(object);
            Log.writeln("# " + object + " -> " + rtn);
        }
        if (Header.testAndMark(rtn)) {
            Address region = MarkRegion.of(rtn.toAddress());
            MarkRegion.setUsedSize(region, MarkRegion.usedSize(region) + VM.objectModel.getSizeWhenCopied(rtn));
            trace.processNode(rtn);
        }
        return rtn;
    }

    /**
     * Trace an object under a copying collection policy.
     * If the object is already copied, the copy is returned.
     * Otherwise, a copy is created and returned.
     * In either case, the object will be marked on return.
     *
     * @param trace The trace being conducted.
     * @param object The object to be forwarded.
     * @return The forwarded object.
     */
    @Inline
    public ObjectReference traceRelocateObject(TraceLocal trace, ObjectReference object, int allocator) {
        /* Race to be the (potential) forwarder */
        Word priorStatusWord = ForwardingWord.attemptToForward(object);
        if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
            /* We lost the race; the object is either forwarded or being forwarded by another thread. */
            /* Note that the concurrent attempt to forward the object may fail, so the object may remain in-place */
            ObjectReference rtn = ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
            if (VM.VERIFY_ASSERTIONS && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(rtn));
            Log.writeln("# " + object + " -> " + rtn);
            return rtn;
        } else {
            /* the object is unforwarded, either because this is the first thread to reach it, or because the object can't be forwarded */
            byte priorState = (byte) (priorStatusWord.toInt() & 0xFF);
            if (Header.isMarked(priorState)) {
                /* the object has not been forwarded, but has the correct mark state; unlock and return unmoved object */
                Header.returnToPriorStateAndEnsureUnlogged(object, priorState); // return to uncontested state
                if (VM.VERIFY_ASSERTIONS && Plan.NEEDS_LOG_BIT_IN_HEADER) VM.assertions._assert(HeaderByte.isUnlogged(object));
                return object;
            } else {
                /* we are the first to reach the object; either mark in place or forward it */
                ObjectReference rtn = object;
                if (MarkRegion.relocationRequired(MarkRegion.of(object.toAddress()))) {
                    /* forward */
                    rtn = ForwardingWord.forwardObject(object, allocator);
                    if (VM.VERIFY_ASSERTIONS && Plan.NEEDS_LOG_BIT_IN_HEADER) VM.assertions._assert(HeaderByte.isUnlogged(rtn));
                    Log.writeln("# " + object + " => " + rtn);
                } else {
                    Header.setMarkStateUnlogAndUnlock(object, priorState);
                    if (VM.VERIFY_ASSERTIONS && Plan.NEEDS_LOG_BIT_IN_HEADER) VM.assertions._assert(HeaderByte.isUnlogged(rtn));
                }
                trace.processNode(rtn);
                return rtn;
            }
        }
    }

    /**
     * Non-atomic read of forwarding pointer
     *
     * @param object The object whose forwarding pointer is to be read
     * @return The forwarding pointer stored in <code>object</code>'s
     * header.
     */
    @Inline
    public static ObjectReference getForwardingPointer(ObjectReference object) {
        return VM.objectModel.readAvailableBitsWord(object).and(Word.fromIntZeroExtend(ForwardingWord.FORWARDING_MASK).not()).toAddress().toObjectReference();
    }

    /**
     * Generic test of the liveness of an object
     *
     * @param object The object in question
     * @return {@code true} if this object is known to be live (i.e. it is marked)
     */
    @Override
    @Inline
    public boolean isLive(ObjectReference object) {
        return Header.isMarked(object);
    }

}
