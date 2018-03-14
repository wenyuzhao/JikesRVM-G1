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

import static org.mmtk.utility.Constants.CARD_META_PAGES_PER_REGION;
import static org.mmtk.utility.Constants.LOG_BYTES_IN_PAGE;

import org.mmtk.plan.Plan;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.plan.zgc.ZGC;
import org.mmtk.plan.zgc.ZGCCollector;
import org.mmtk.policy.Space;
import org.mmtk.utility.*;
import org.mmtk.utility.heap.*;
import org.mmtk.utility.options.LineReuseRatio;
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

    public static final int LOCAL_GC_BITS_REQUIRED = 2;
    public static final int GLOBAL_GC_BITS_REQUIRED = 0;
    public static final int GC_HEADER_WORDS_REQUIRED = 0;

    private static final int META_DATA_PAGES_PER_REGION = CARD_META_PAGES_PER_REGION;
    private static final Offset FORWARDING_POINTER_OFFSET = VM.objectModel.GC_HEADER_OFFSET();

    /**
     *
     */
    //private static short reusableMarkStateThreshold = 0;

    /****************************************************************************
     *
     * Instance variables
     */

    /**
     *
     */
    //private byte markState = ZObjectHeader.MARK_BASE_VALUE;
    //byte lineMarkState = RESET_LINE_MARK_STATE;
    //private byte lineUnavailState = RESET_LINE_MARK_STATE;
    private boolean inCollection;
    //private int linesConsumed = 0;

    //private final Lock mutatorLock = VM.newLock(getName() + "mutator");
    //private final Lock gcLock = VM.newLock(getName() + "gc");

    //private Address allocBlockCursor = Address.zero();
    //private Address allocBlockSentinel = Address.zero();
    //private boolean exhaustedReusableSpace = true;

    //private final ChunkList chunkMap = new ChunkList();
    //private final AddressArray blocks = AddressArray.create(1 << 4);
    //private final Defrag defrag;

    /****************************************************************************
     *
     * Initialization
     */

    static {
        //Options.lineReuseRatio = new LineReuseRatio();
        //reusableMarkStateThreshold = (short) (Options.lineReuseRatio.getValue() * MAX_BLOCK_MARK_STATE);
    }

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
            pr = new FreeListPageResource(this, 0);
        else
            pr = new FreeListPageResource(this, start, extent, 0);
        collectionReserve = Math.round(pr.getAvailablePhysicalPages() * 0.05f);
        // defrag = new Defrag((FreeListPageResource) pr);
    }

    /****************************************************************************
     *
     * Global prepare and release
     */

    /**
     * Prepare for a new collection increment.
     */
    public void prepare() {
        ZObjectHeader.deltaMarkState(true);
        inCollection = true;
    }

    /**
     * A new collection increment has completed.  Release global resources.
     */
    public void release() {
        inCollection = false;
    }

    /**
     * Return the number of pages reserved for copying.
     */
    public int getCollectionReserve() {
        return collectionReserve;
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

    Address copyPage = null;
    int collectionReserve = 0;
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
        // If its the first time to allocate, reserve 5% pages for copying
        /*int targetCollectionReserve = Math.round(pr.getAvailablePhysicalPages() * 0.05f);
        if (collectionReserve < targetCollectionReserve) {
            Log.writeln("Try to reserve " + (targetCollectionReserve - collectionReserve) + " pages from total " + pr.getAvailablePhysicalPages() + " pages");
            collectionReserve = pr.reservePages(targetCollectionReserve - collectionReserve);
        }
        reser*/
        // Allocate
        Address zPage;
        if (copy) {
            zPage = pr.getNewPages(ZPage.PAGES, ZPage.PAGES, true);
            if (!zPage.isZero()) collectionReserve -= ZPage.PAGES;
        } else {
            zPage = acquire(ZPage.PAGES);
        }

        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ZPage.isAligned(zPage));

        if (!zPage.isZero()) {
            VM.memory.zero(false, zPage, Extent.fromIntZeroExtend(ZPage.BYTES));

            ZPage.fromPages.push(zPage);
            Log.writeln("#ZPage alloc " + zPage);
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
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ZPage.isAligned(zPage));
        ZPage.fromPages.remove(zPage);
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
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ZObjectHeader.isNewObject(object));
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
        ZObjectHeader.writeMarkState(object, ZObjectHeader.markState);
        ForwardingWord.clearForwardingBits(object);
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

        ObjectReference rtn = object;

        if (ForwardingWord.isForwarded(object)) {
            Word statusWord = ForwardingWord.attemptToForward(object);
            ObjectReference newObject = ForwardingWord.extractForwardingPointer(statusWord);
            // Log.writeln("# -> ", newObject);
            rtn = newObject;
        }

        if (ZObjectHeader.testAndMark(rtn, ZObjectHeader.markState) != ZObjectHeader.markState) {
            Address zPage = ZPage.of(rtn.toAddress());
            ZPage.setUsedSize(zPage, ZPage.usedSize(zPage) + VM.objectModel.getSizeWhenCopied(rtn));
            trace.processNode(rtn);
        }

        return rtn;

        /*
        ObjectReference rtn = object;
        byte markValue = ZObjectHeader.markState;
        byte oldMarkState = ZObjectHeader.testAndMark(object, markValue);
        if (oldMarkState != markValue) {
            trace.processNode(object);
        }
        //Word priorStatusWord = ForwardingWord.attemptToForward(object);
        //if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
        if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
            Log.writeln("#isForwardedOrBeingForwarded " + object.toAddress());
        //    rtn = ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
        }

        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
        // if (VM.VERIFY_ASSERTIONS  && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(object));

        if (VM.VERIFY_ASSERTIONS) {
            VM.assertions._assert(!rtn.isNull());
            //VM.assertions._assert(defrag.spaceExhausted() || !isDefragSource(rtn) || (ObjectHeader.isPinnedObject(rtn)));
        }
        // Inc zPage used size
        Address zPage = ZPage.of(rtn.toAddress());
        ZPage.setUsedSize(zPage, ZPage.usedSize(zPage) + VM.objectModel.getSizeWhenCopied(rtn));

        return rtn;
        */
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
        //Log.writeln("###traceObjectWithCopy");
        //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((nurseryCollection && !ZObjectHeader.isMatureObject(object)) || (defrag.determined(true) && isDefragSource(object)));
        /* Race to be the (potential) forwarder */
        Word priorStatusWord = ForwardingWord.attemptToForward(object);
        if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
            /* We lost the race; the object is either forwarded or being forwarded by another thread. */
            /* Note that the concurrent attempt to forward the object may fail, so the object may remain in-place */
            ObjectReference rtn = ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
            if (VM.VERIFY_ASSERTIONS && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(rtn));
            return rtn;
        } else {
            byte priorState = (byte) (priorStatusWord.toInt() & 0xFF);
            /* the object is unforwarded, either because this is the first thread to reach it, or because the object can't be forwarded */
            if (ZObjectHeader.testMarkState(priorState, ZObjectHeader.markState)) {
                /* the object has not been forwarded, but has the correct mark state; unlock and return unmoved object */
                /* Note that in a sticky mark bits collector, the mark state does not change at each GC, so correct mark state does not imply another thread got there first */
                //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(nurseryCollection || defrag.spaceExhausted() || ObjectHeader.isPinnedObject(object));
                ZObjectHeader.returnToPriorStateAndEnsureUnlogged(object, priorState); // return to uncontested state
                if (VM.VERIFY_ASSERTIONS && Plan.NEEDS_LOG_BIT_IN_HEADER) VM.assertions._assert(HeaderByte.isUnlogged(object));
                return object;
            } else {
                /* we are the first to reach the object; either mark in place or forward it */
                ObjectReference rtn = object;
                if (ZPage.relocationRequired(ZPage.of(object.toAddress()))) {
                    /* forward */
                    //Log.writeln("#Forwarding " + object + ", curr size " + VM.objectModel.getCurrentSize(object) + " copy size " + VM.objectModel.getSizeWhenCopied(object));
                    ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
                    if (VM.VERIFY_ASSERTIONS && Plan.NEEDS_LOG_BIT_IN_HEADER)
                        VM.assertions._assert(HeaderByte.isUnlogged(newObject));

                    //Log.writeln("#Forward " + object.toAddress() + " -> " + newObject.toAddress());

                    rtn = newObject;
                } else {
                    ZObjectHeader.setMarkStateUnlogAndUnlock(object, priorState, ZObjectHeader.markState);
                }
                trace.processNode(rtn);
                return rtn;
            }
        }
    }

    /****************************************************************************
     *
     * Object state
     */

    /**
     * Generic test of the liveness of an object
     *
     * @param object The object in question
     * @return {@code true} if this object is known to be live (i.e. it is marked)
     */
    @Override
    @Inline
    public boolean isLive(ObjectReference object) {
        return ZObjectHeader.testMarkState(object, ZObjectHeader.markState);
    }

}
