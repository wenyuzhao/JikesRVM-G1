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
package org.mmtk.policy.zgc;

import org.mmtk.plan.Plan;
import org.mmtk.plan.TraceLocal;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.MarkRegion;
import org.mmtk.policy.Space;
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
public final class ZSpace extends Space {

    /****************************************************************************
     *
     * Class variables
     */
    public static final int LOCAL_GC_BITS_REQUIRED = 1 + ForwardingWord.FORWARDING_BITS;
    public static final int GLOBAL_GC_BITS_REQUIRED = 0;
    public static final int GC_HEADER_WORDS_REQUIRED = 0;

    private static final Word GC_MARK_BIT_MASK = Word.one().lsh(2);
    private static final Word MARK_MASK = Word.fromIntZeroExtend(1 << ForwardingWord.FORWARDING_MASK);
    private static final Word MARK_AND_FORWARD_MASK = MARK_MASK.or(Word.fromIntZeroExtend(ForwardingWord.FORWARDING_MASK));
    private static final Offset FORWARDING_POINTER_OFFSET = VM.objectModel.GC_HEADER_OFFSET();

    /**
     * The caller specifies the region of virtual memory to be used for
     * this space.  If this region conflicts with an existing space,
     * then the constructor will fail.
     *
     * @param name The name of this space (used when printing error messages etc)
     * @param vmRequest The virtual memory request
     */
    public ZSpace(String name, VMRequest vmRequest) {
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
    public ZSpace(String name, boolean zeroed, VMRequest vmRequest) {
        super(name, false, false, zeroed, vmRequest);
        if (vmRequest.isDiscontiguous())
            pr = new FreeListPageResource(this, MarkRegion.METADATA_PAGES_PER_MMTK_REGION);
        else
            pr = new FreeListPageResource(this, start, extent, MarkRegion.METADATA_PAGES_PER_MMTK_REGION);
    }

    /****************************************************************************
     *
     * Global prepare and release
     */

    /**
     * Prepare for a new collection increment.
     */
    public void prepare() {
        //ZObjectHeader.deltaMarkState(true);
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

    /****************************************************************************
     *
     * Allocation
     */

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
        Address zPage = acquire(MarkRegion.PAGES_IN_REGION);

        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkRegion.isAligned(zPage));

        if (!zPage.isZero()) {
            VM.memory.zero(false, zPage, Extent.fromIntZeroExtend(MarkRegion.BYTES_IN_REGION));;
            Log.writeln("#Block alloc " + zPage + ", in region " + EmbeddedMetaData.getMetaDataBase(zPage));
            MarkRegion.register(zPage);
        }
        return zPage;
    }

    /**
     * Release a block.  A block is free, so call the underlying page allocator
     * to release the associated storage.
     *
     * @param zPage The address of the Z Page to be released
     */
    @Override
    @Inline
    public void release(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkRegion.isAligned(zPage));
        MarkRegion.unregister(zPage);
        ((FreeListPageResource) pr).releasePages(zPage);
    }

    /****************************************************************************
     *
     * Header manipulation
     */

    /**
     * Perform any required post allocation initialization
     *
     * @param object the object ref to the storage to be initialized
     * @param bytes size of the allocated object in bytes
     */
    @Inline
    public void postAlloc(ObjectReference object, int bytes) {
        // if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ZObjectHeader.isNewObject(object));
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
        ForwardingWord.clearForwardingBits(object);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
        if (VM.VERIFY_ASSERTIONS && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(object));
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
        if (testAndMark(rtn)) {
            Address zPage = MarkRegion.of(rtn.toAddress());
            MarkRegion.setUsedSize(zPage, MarkRegion.usedSize(zPage) + VM.objectModel.getSizeWhenCopied(rtn));
            trace.processNode(rtn);
        }
        return rtn;
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
    public ObjectReference traceRelocateObject(TraceLocal trace, ObjectReference object, int allocator) {
        // Log.writeln("TraceRelocateObject " + object + " " + ForwardingWord.isForwardedOrBeingForwarded(object));
        /* Race to be the (potential) forwarder */
        Word priorStatusWord = ForwardingWord.attemptToForward(object);
        if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
            /* We lost the race; the object is either forwarded or being forwarded by another thread. */
            /* Note that the concurrent attempt to forward the object may fail, so the object may remain in-place */
            clearMark(object);
            Word forwardingWord = ForwardingWord.attemptToForward(object);
            ObjectReference rtn = ForwardingWord.spinAndGetForwardedObject(object, forwardingWord);
            if (VM.VERIFY_ASSERTIONS && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(rtn));
            Log.writeln("# " + object + " -> " + rtn);
            return rtn;
        } else {
            /* the object is unforwarded, either because this is the first thread to reach it, or because the object can't be forwarded */
            if (testAndClearMark(object)) {
                /* we are the first to reach the object; either mark in place or forward it */
                ObjectReference rtn = object;
                if (MarkRegion.relocationRequired(MarkRegion.of(object.toAddress()))) {
                    /* forward */
                    rtn = ForwardingWord.forwardObject(object, allocator);
                    Log.writeln("# " + object + " => " + rtn);
                    if (VM.VERIFY_ASSERTIONS && Plan.NEEDS_LOG_BIT_IN_HEADER)
                        VM.assertions._assert(HeaderByte.isUnlogged(rtn));
                } else {
                    Log.writeln("# " + object);
                    ForwardingWord.clearForwardingBits(rtn);
                }
                trace.processNode(rtn);
                return rtn;
            } else {
                if (VM.VERIFY_ASSERTIONS && Plan.NEEDS_LOG_BIT_IN_HEADER)
                    VM.assertions._assert(HeaderByte.isUnlogged(object));
                ForwardingWord.clearForwardingBits(object);
                Log.writeln("# " + object + " marked");
                return object;
            }
        }
    }

    /****************************************************************************
     *
     * Object state
     */

    /**
     * Non-atomic read of forwarding pointer
     *
     * @param object The object whose forwarding pointer is to be read
     * @return The forwarding pointer stored in <code>object</code>'s
     * header.
     */
    @Inline
    public static ObjectReference getForwardingPointer(ObjectReference object) {
        return VM.objectModel.readAvailableBitsWord(object).and(MARK_AND_FORWARD_MASK.not()).toAddress().toObjectReference();
    }

    @Inline
    public static void clearMark(ObjectReference object) {
        Word oldValue = VM.objectModel.readAvailableBitsWord(object);
        VM.objectModel.writeAvailableBitsWord(object, oldValue.and(MARK_MASK.not()));
    }

    /**
     * Used to mark boot image objects during a parallel scan of objects
     * during GC.
     *
     * @param object The object to be marked
     * @return {@code true} if marking was done.
     */
    @Inline
    public static boolean testAndMark(ObjectReference object) {
        Word oldValue;
        do {
            oldValue = VM.objectModel.prepareAvailableBits(object);
            Word markBit = oldValue.and(MARK_MASK);
            if (!markBit.isZero()) return false;
        } while (!VM.objectModel.attemptAvailableBits(object, oldValue, oldValue.or(MARK_MASK)));
        return true;
    }

    /**
     * Used to mark boot image objects during a parallel scan of objects
     * during GC Returns true if marking was done.
     *
     * @param object The object to be marked
     * @return {@code true} if marking was done, {@code false} otherwise
     */
    @Inline
    private static boolean testAndClearMark(ObjectReference object) {
        Word oldValue;
        do {
            oldValue = VM.objectModel.prepareAvailableBits(object);
            Word markBit = oldValue.and(MARK_MASK);
            if (markBit.isZero()) return false;
        } while (!VM.objectModel.attemptAvailableBits(object, oldValue, oldValue.and(MARK_MASK.not())));
        return true;
    }

    /**
     * @param object the object in question
     * @return {@code true} if the object is marked
     */
    @Inline
    public static boolean isMarked(ObjectReference object) {
        Word oldValue = VM.objectModel.readAvailableBitsWord(object);
        Word markBit = oldValue.and(MARK_MASK);
        return (!markBit.isZero());
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
        return true;
    }

}
