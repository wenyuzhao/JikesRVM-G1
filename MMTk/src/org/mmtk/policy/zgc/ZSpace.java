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
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.Space;
import org.mmtk.utility.*;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.heap.*;

import org.mmtk.utility.heap.layout.HeapLayout;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.Lock;
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

    public static final int LOCAL_GC_BITS_REQUIRED = 1;
    public static final int GLOBAL_GC_BITS_REQUIRED = 0;
    public static final int GC_HEADER_WORDS_REQUIRED = 1;
    private static final Word GC_MARK_BIT_MASK = Word.one();

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
            pr = new FreeListPageResource(this, Block.METADATA_PAGES_PER_REGION);
        else
            pr = new FreeListPageResource(this, start, extent, Block.METADATA_PAGES_PER_REGION);
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
        Address zPage = acquire(Block.PAGES_IN_BLOCK);

        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Block.isAligned(zPage));

        if (!zPage.isZero()) {
            VM.memory.zero(false, zPage, Extent.fromIntZeroExtend(Block.BYTES_IN_BLOCK));;
            Log.writeln("#Block alloc " + zPage + ", in region " + EmbeddedMetaData.getMetaDataBase(zPage));
            Block.setAllocated(zPage, true);
            if (Block.firstRegion == null) Block.firstRegion = EmbeddedMetaData.getMetaDataBase(zPage);
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
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Block.isAligned(zPage));
        Block.clearState(zPage);
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
        //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ZObjectHeader.isNewObject(object));
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
        // ZObjectHeader.writeMarkState(object, ZObjectHeader.markState);
        //ForwardingWord.clearForwardingBits(object);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
        if (VM.VERIFY_ASSERTIONS && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(object));
    }

    /****************************************************************************
     *
     * Object tracing
     */

    @Inline
    public static ObjectReference getForwardedObject(ObjectReference object) {
        return object.toAddress().loadObjectReference(FORWARDING_POINTER_OFFSET);
    }

    @Inline
    public static boolean testAndMark(ObjectReference object) {
        Word oldValue;
        do {
            oldValue = VM.objectModel.prepareAvailableBits(object);
            Word markBit = oldValue.and(GC_MARK_BIT_MASK);
            if (!markBit.isZero()) return false;
        } while (!VM.objectModel.attemptAvailableBits(object, oldValue, oldValue.or(GC_MARK_BIT_MASK)));
        return true;
    }

    @Inline
    private static boolean testAndClearMark(ObjectReference object) {
        Word oldValue;
        do {
            oldValue = VM.objectModel.prepareAvailableBits(object);
            Word markBit = oldValue.and(GC_MARK_BIT_MASK);
            if (markBit.isZero()) return false;
        } while (!VM.objectModel.attemptAvailableBits(object, oldValue, oldValue.and(GC_MARK_BIT_MASK.not())));
        return true;
    }

    /**
     * Trace a reference to an object.  If the object header is not already
     * marked, mark the object and enqueue it for subsequent processing.
     *
     * @param trace The trace performing the transitive closure
     * @param object The object to be traced.
     * @param allocator The allocator to which any copying should be directed
     * @return The object, which may have been moved.
     */
    @Inline
    public ObjectReference traceMarkObject(TransitiveClosure trace, ObjectReference object, int allocator) {
        //Log.writeln("###traceObject");
        //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(defrag.determined(true));
        if (testAndMark(object)) {
            Address zPage = Block.of(object.toAddress());
            Block.setUsedSize(zPage, Block.usedSize(zPage) + VM.objectModel.getSizeWhenCopied(object));
            trace.processNode(object);
        } else if (!getForwardedObject(object).isNull()) {
            return getForwardedObject(object);
        }
        return object;


        /*
        ObjectReference rtn = object;

        if (ForwardingWord.isForwarded(object)) {
            Word statusWord = ForwardingWord.attemptToForward(object);
            ObjectReference newObject = ForwardingWord.extractForwardingPointer(statusWord);
            // Log.writeln("# -> ", newObject);
            rtn = newObject;
        }
        //lock.acquire();
        if (ZObjectHeader.testAndMark(rtn, ZObjectHeader.markState) != ZObjectHeader.markState) {
            Address zPage = Block.of(rtn.toAddress());
            Block.setUsedSize(zPage, Block.usedSize(zPage) + VM.objectModel.getSizeWhenCopied(rtn));
            trace.processNode(rtn);
        }
        //lock.release();
        return rtn;*/
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
     * Trace a reference to an object, forwarding the object if appropriate
     * If the object is not already marked, mark the object and enqueue it
     * for subsequent processing.
     *
     * @param trace The trace performing the transitive closure
     * @param object The object to be traced.
     * @param allocator The allocator to which any copying should be directed
     * @return Either the object or a forwarded object, if it was forwarded.
     */
    @Inline
    public ObjectReference traceRelocateObject(TransitiveClosure trace, ObjectReference object, int allocator) {
        Word forwardingWord = ForwardingWord.attemptToForward(object);

        if (ForwardingWord.stateIsForwardedOrBeingForwarded(forwardingWord)) {
            ObjectReference rtn = ForwardingWord.spinAndGetForwardedObject(object, forwardingWord);
            return rtn;
        } else {
            if (testAndClearMark(object)) {
                ObjectReference newObject = object;//forwardObjectIfRequired(object, allocator);
                trace.processNode(newObject);
                return newObject;
            } else {
                return object;
            }
        }
    }

    @Inline
    public ObjectReference forwardObjectIfRequired(ObjectReference object, int allocator) {
        if (Block.relocationRequired(Block.of(object.toAddress()))) {
            /* forward */
            //Log.writeln("#Forwarding " + object + ", curr size " + VM.objectModel.getCurrentSize(object) + " copy size " + VM.objectModel.getSizeWhenCopied(object));
            ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(HeaderByte.isUnlogged(newObject));

            ForwardingWord.clearForwardingBits(newObject);
            //Log.writeln("#Forward " + object.toAddress() + " -> " + newObject.toAddress());

            return newObject;
        } else {
            clearMark(object);
            return object;//ZObjectHeader.setMarkStateUnlogAndUnlock(object, priorState, ZObjectHeader.markState);
        }
        //trace.processNode(rtn);
    }

    /****************************************************************************
     *
     * Object state
     */

    @Inline
    public static boolean isMarked(ObjectReference object) {
        Word oldValue = VM.objectModel.readAvailableBitsWord(object);
        Word markBit = oldValue.and(GC_MARK_BIT_MASK);
        return (!markBit.isZero());
    }

    @Inline
    public static void clearMark(ObjectReference object) {
        Word oldValue = VM.objectModel.readAvailableBitsWord(object);
        VM.objectModel.writeAvailableBitsWord(object, oldValue.and(GC_MARK_BIT_MASK.not()));
    }

    /*
     * Generic test of the liveness of an object
     *
     * @param object The object in question
     * @return {@code true} if this object is known to be live (i.e. it is marked)
     */
    @Override
    @Inline
    public boolean isLive(ObjectReference object) {
        return isMarked(object);
    }

}
