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
import org.mmtk.utility.heap.layout.HeapLayout;
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
public final class MarkBlockSpace extends Space {
  /** number of header bits we may use */
  private static final int AVAILABLE_LOCAL_BITS = 8 - HeaderByte.USED_GLOBAL_BITS;

  /* local status bits */
  private static final byte NEW_OBJECT_MARK = 0; // using zero means no need for explicit initialization on allocation
  private static final int  MARK_BASE = ForwardingWord.FORWARDING_BITS;
  private static final int  MAX_MARKCOUNT_BITS = AVAILABLE_LOCAL_BITS - MARK_BASE;
  private static final byte MARK_INCREMENT = 1 << MARK_BASE;
  private static final byte MARK_MASK = (byte) (((1 << MAX_MARKCOUNT_BITS) - 1) << MARK_BASE);
  private static final byte MARK_AND_FORWARDING_MASK = (byte) (MARK_MASK | ForwardingWord.FORWARDING_MASK);
  private static final byte MARK_BASE_VALUE = MARK_INCREMENT;

  public static final int LOCAL_GC_BITS_REQUIRED = AVAILABLE_LOCAL_BITS;
  public static final int GLOBAL_GC_BITS_REQUIRED = 0;
  public static final int GC_HEADER_WORDS_REQUIRED = 0;

  public  Address allocBlock = Address.zero();

  @Uninterruptible
  public static class Header {
    static byte markState = MARK_BASE_VALUE;
    static boolean isNewObject(ObjectReference object) {
      return (VM.objectModel.readAvailableByte(object) & MARK_AND_FORWARDING_MASK) == NEW_OBJECT_MARK;
    }
    static boolean isMarked(ObjectReference object) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((markState & MARK_MASK) == markState);
      return isMarked(VM.objectModel.readAvailableByte(object));
    }
    static boolean isMarked(byte gcByte) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((markState & MARK_MASK) == markState);
      return ((byte) (gcByte & MARK_MASK)) == markState;
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
    static void increaseMarkState() {
      byte rtn = markState;
      do {
        rtn = (byte) (rtn + MARK_INCREMENT);
        rtn &= MARK_MASK;
      } while (rtn < MARK_BASE_VALUE);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(rtn != markState);
      markState = rtn;
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
    public static void mark(ObjectReference object) {
      testAndMark(object);
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
  }

  /**
   * The caller specifies the region of virtual memory to be used for
   * this space.  If this region conflicts with an existing space,
   * then the constructor will fail.
   *
   * @param name The name of this space (used when printing error messages etc)
   * @param vmRequest The virtual memory request
   */
  public MarkBlockSpace(String name, VMRequest vmRequest) {
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
  public MarkBlockSpace(String name, boolean zeroed, VMRequest vmRequest) {
    super(name, true, false, zeroed, vmRequest);
    if (vmRequest.isDiscontiguous())
      pr = new FreeListPageResource(this, MarkBlock.METADATA_PAGES_PER_REGION);
    else
      pr = new FreeListPageResource(this, start, extent, MarkBlock.METADATA_PAGES_PER_REGION);
  }

  @Override
  public void growSpace(Address start, Extent bytes, boolean newChunk) {
    if (newChunk) {
      Address chunk = Conversions.chunkAlign(start.plus(bytes), true);
      if (VM.VERIFY_ASSERTIONS) Log.writeln("#New Region ", chunk);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Conversions.chunkAlign(start.plus(bytes), true).EQ(chunk));
      // MarkBlock.clearRegionMetadata(chunk);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(chunk.EQ(EmbeddedMetaData.getMetaDataBase(chunk)));
      HeapLayout.mmapper.ensureMapped(chunk, MarkBlock.METADATA_PAGES_PER_REGION);
      VM.memory.zero(false, chunk, Extent.fromIntZeroExtend(MarkBlock.BLOCKS_START_OFFSET));
    }
  }

  /**
   * Prepare for a new collection increment.
   */
  public void prepare() {
    Header.increaseMarkState();
  }

  /**
   * A new collection increment has completed.  Release global resources.
   */
  public void release() {}

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
        Address region = acquire(MarkBlock.PAGES_IN_BLOCK);

        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.isAligned(region));

        if (VM.VERIFY_ASSERTIONS) {
            Log.write("#Block alloc ", region);
            Log.writeln(", in region ", EmbeddedMetaData.getMetaDataBase(region));
        }

        if (!region.isZero()) {
            //int oldCount = MarkBlock.count();
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!MarkBlock.allocated(region));
            MarkBlock.register(region);
            //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.count() == oldCount + 1);
        }
        if (VM.VERIFY_ASSERTIONS) {
            if (!region.isZero()) {
                VM.assertions._assert(MarkBlock.allocated(region));
                VM.assertions._assert(!MarkBlock.relocationRequired(region));
                VM.assertions._assert(MarkBlock.usedSize(region) == 0);
            } else {
                Log.writeln("ALLOCATED A NULL REGION");
            }
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
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.isAligned(region));

      //int oldCount = MarkBlock.count();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.allocated(region));
      MarkBlock.unregister(region);
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.count() == oldCount - 1);

      ((FreeListPageResource) pr).releasePages(region);
    }

    static public boolean updateNewObjectSize = true;
    /**
     * Perform any required post allocation initialization
     *
     * @param object the object ref to the storage to be initialized
     * @param bytes size of the allocated object in bytes
     */
    @Inline
    public void postAlloc(ObjectReference object, int bytes) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Header.isNewObject(object));

        if (Header.testAndMark(object)) {
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!object.isNull());
            /*if (updateNewObjectSize) {
                Address region = MarkBlock.of(VM.objectModel.objectStartRef(object));
                MarkBlock.setUsedSize(region, MarkBlock.usedSize(region) + VM.objectModel.getCurrentSize(object));
            }*/
        }
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Header.isMarked(object));
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

      //VM.objectModel.writeAvailableByte(object, (byte) (VM.objectModel.readAvailableByte(object) & ~ForwardingWord.FORWARDING_MASK));
      /*Log.writeln("Post Copy ", object);
      Log.writeln("Header Word ", VM.objectModel.readAvailableBitsWord(object));
      Log.writeln("Header Byte ", VM.objectModel.readAvailableByte(object));
      Log.writeln("Forward state ", VM.objectModel.readAvailableByte(object) & ForwardingWord.FORWARDING_MASK);*/
      //VM.objectModel.dumpObject(object);
      //VM.objectModel.writeAvailableByte(object, (byte) (VM.objectModel.readAvailableByte(object) & ~ForwardingWord.FORWARDING_MASK));
      // Log.writeln("Forward state ", VM.objectModel.readAvailableByte(object) & ForwardingWord.FORWARDING_MASK);
      Header.writeMarkState(object);
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
    Lock lock = VM.newLock("asfsgsdfsa");
    @Inline
    public ObjectReference traceMarkObject(TransitiveClosure trace, ObjectReference object) {
        //VM.objectModel.dumpObject(object);
        //Log.flush();
      lock.acquire();
        ObjectReference rtn = object;
        if (ForwardingWord.isForwarded(object)) {
          //rtn = ForwardingWord.spinAndGetForwardedObject(object, VM.objectModel.prepareAvailableBits(object));
            rtn = getForwardingPointer(object);
            if (VM.VERIFY_ASSERTIONS) {
              Log.write("# ", object);
              Log.writeln(" -> ", rtn);
              Log.flush();
            }
        }
        if (VM.VERIFY_ASSERTIONS) {
            if (ForwardingWord.isForwardedOrBeingForwarded(rtn)) {
                Log.write(rtn);
                Log.write(ForwardingWord.isForwarded(rtn) ? " isForwarded" : " isBeingForwarded");
                Log.writeln(" -> ", getForwardingPointer(rtn));
            }
            VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(rtn));
        }

        if (Header.testAndMark(rtn)) {
            Address region = MarkBlock.of(VM.objectModel.objectStartRef(rtn));
            if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!rtn.isNull());
            //VM.objectModel.dumpObject(rtn);
            MarkBlock.setUsedSize(region, MarkBlock.usedSize(region) + VM.objectModel.getCurrentSize(rtn));
          lock.release();
            trace.processNode(rtn);
        } else {
          lock.release();
        }

        // if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
        if (VM.VERIFY_ASSERTIONS  && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(rtn));

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
            //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(rtn));
            if (VM.VERIFY_ASSERTIONS && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(rtn));
            //Log.write("# ", object);
            //Log.writeln(" -> ", rtn);
            return rtn;
        } else {
            /* the object is unforwarded, either because this is the first thread to reach it, or because the object can't be forwarded */
            byte priorState = (byte) (priorStatusWord.toInt() & 0xFF);
            if (Header.isMarked(priorState)) {
                /* the object has not been forwarded, but has the correct mark state; unlock and return unmoved object */
                Header.returnToPriorStateAndEnsureUnlogged(object, priorState); // return to uncontested state
                if (VM.VERIFY_ASSERTIONS && Plan.NEEDS_LOG_BIT_IN_HEADER) VM.assertions._assert(HeaderByte.isUnlogged(object));
                /*if (VM.VERIFY_ASSERTIONS) {
                  if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
                    Log.write("ERROR ");
                    Log.write(ForwardingWord.isForwarded(object) ? " forwarded" : " being forwarded");
                    if (ForwardingWord.isForwarded(object)) Log.write(" to ", getForwardingPointer(object));
                    Log.write(" header ", priorStatusWord.toAddress());
                    Log.writeln(" priorState ", priorState);
                  }
                  //VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
                }*/
                return object;
            } else {
                /* we are the first to reach the object; either mark in place or forward it */
                ObjectReference rtn = object;
                if (MarkBlock.relocationRequired(MarkBlock.of(VM.objectModel.objectStartRef(object)))) {
                    /* forward */
                    rtn = ForwardingWord.forwardObject(object, allocator);
                    if (VM.VERIFY_ASSERTIONS) {
                        VM.assertions._assert(ForwardingWord.isForwarded(object));
                        VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(rtn));
                        if (Plan.NEEDS_LOG_BIT_IN_HEADER) VM.assertions._assert(HeaderByte.isUnlogged(rtn));
                    }
                    if (VM.VERIFY_ASSERTIONS) {
                        Log.write("# ", object);
                        Log.writeln(" => ", rtn);
                    }
                } else {
                    Header.setMarkStateUnlogAndUnlock(object, priorState);
                    if (VM.VERIFY_ASSERTIONS) {
                        //VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(rtn));
                        if (Plan.NEEDS_LOG_BIT_IN_HEADER) VM.assertions._assert(HeaderByte.isUnlogged(rtn));
                    }
                }
                trace.processNode(rtn);
                return rtn;
            }
        }
    }

    @Inline
    public static ObjectReference getForwardingPointer(ObjectReference object) {
        Word statusWord = VM.objectModel.readAvailableBitsWord(object);
        return statusWord.and(Word.fromIntZeroExtend(ForwardingWord.FORWARDING_MASK).not()).toAddress().toObjectReference();
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

    // Block iterator

    public Address firstBlock() {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!contiguous);
        return nextBlock(headDiscontiguousRegion);
    }

    public Address nextBlock(Address block) {
        if (VM.VERIFY_ASSERTIONS) {
            VM.assertions._assert(!contiguous);
            if (!(block.EQ(EmbeddedMetaData.getMetaDataBase(block)) || (MarkBlock.indexOf(block) >= 0 && MarkBlock.indexOf(block) < MarkBlock.BLOCKS_IN_REGION))) {
                Log.write("Invalid block ", block);
                Log.write(" at region ", EmbeddedMetaData.getMetaDataBase(block));
                Log.write(" with index ", MarkBlock.indexOf(block));
                Log.writeln(MarkBlock.isAligned(block) ? " aligned" : " not aligned");
            }
            VM.assertions._assert(block.EQ(EmbeddedMetaData.getMetaDataBase(block)) || (MarkBlock.indexOf(block) >= 0 && MarkBlock.indexOf(block) < MarkBlock.BLOCKS_IN_REGION));

        }
        int i = block.EQ(EmbeddedMetaData.getMetaDataBase(block)) ? 0 : MarkBlock.indexOf(block) + 1;
        Address region = EmbeddedMetaData.getMetaDataBase(block);
        if (i >= MarkBlock.BLOCKS_IN_REGION) {
            i = 0;
            region = HeapLayout.vmMap.getNextContiguousRegion(region);//region.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress();
        }

        while (true) {
            if (region.isZero()) return null;

            Address allocated = region.plus(MarkBlock.METADATA_OFFSET_IN_REGION + i * MarkBlock.METADATA_BYTES + MarkBlock.METADATA_ALLOCATED_OFFSET);
            if (allocated.loadByte() != ((byte) 0)) {
                Address rtn = region.plus(MarkBlock.BLOCKS_START_OFFSET + i * MarkBlock.BYTES_IN_BLOCK);
                return rtn;
            }

            i++;
            if (i >= MarkBlock.BLOCKS_IN_REGION) {
                i = 0;
                region = HeapLayout.vmMap.getNextContiguousRegion(region);
            }
        }
    }

    public void selectRelocationBlocks() {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!contiguous);

      // Perform relocation set selection
      int blocksCount = MarkBlock.count();
      if (VM.VERIFY_ASSERTIONS) Log.writeln("Blocks: ", blocksCount);
      AddressArray blocks = AddressArray.create(blocksCount);
      AddressArray blockSizes = AddressArray.create(blocksCount);

      // Initialize blocks array
      {
        int index = 0;
        Address block = firstBlock();
        while (block != null) {
          if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero());
          if (index >= blocks.length()) break;
          blocks.set(index, block);
          blockSizes.set(index, Address.fromIntZeroExtend(MarkBlock.usedSize(block)));
          block = nextBlock(block);
          index++;
        }
        // if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.count() == index);
        // blockRegistrationLock.release();
      }

      // quick sort
      AddressArrayQuickSort.sort(blocks, blockSizes);

      // select relocation blocks
      final int useableBytesForCopying = (int) (VM.activePlan.global().getPagesAvail() * (1.0 - MarkBlock.METADATA_PAGES_PER_REGION / EmbeddedMetaData.PAGES_IN_REGION) * Constants.BYTES_IN_PAGE);
      if (VM.VERIFY_ASSERTIONS) Log.writeln("Copy pages: ", useableBytesForCopying / MarkBlock.BYTES_IN_BLOCK);
      int currentSize = 0;
      for (int i = 0; i < blocks.length(); i++) {
        int size = blockSizes.get(i).toInt();
        Address block = blocks.get(i);

        if (VM.VERIFY_ASSERTIONS) {
          Log.write("#Block ", block);
          Log.write(": ", MarkBlock.usedSize(block));
          Log.write("/", MarkBlock.BYTES_IN_BLOCK);
        }
        if (block.EQ(allocBlock)) {
          if (VM.VERIFY_ASSERTIONS) Log.writeln(" alloc");
          continue;
        }
        if (currentSize + size > useableBytesForCopying || size * 2 > MarkBlock.BYTES_IN_BLOCK) {
          if (VM.VERIFY_ASSERTIONS) Log.writeln();
          break;
        }

        if (VM.VERIFY_ASSERTIONS) Log.writeln(" relocate");

        currentSize += size;
        MarkBlock.setRelocationState(block, true);
      }
    }

    @Uninterruptible
    public static class AddressArrayQuickSort {
        @Inline
        public static void sort(AddressArray blocks, AddressArray blockSizes) {
            sort(blocks, blockSizes, 0, blocks.length() - 1);
        }

        // quicksort the subarray from a[lo] to a[hi]
        private static void sort(AddressArray blocks, AddressArray blockSizes, int lo, int hi) {
            if (hi <= lo) return;
            int j = partition(blocks, blockSizes, lo, hi);
            sort(blocks, blockSizes, lo, j-1);
            sort(blocks, blockSizes, j+1, hi);
        }

        // partition the subarray a[lo..hi] so that a[lo..j-1] <= a[j] <= a[j+1..hi]
        // and return the index j.
        @Inline
        private static int partition(AddressArray blocks, AddressArray blockSizes, int lo, int hi) {
            int i = lo, j = hi + 1;
            int size = blockSizes.get(lo).toInt();
            while (true) {
                while (blockSizes.get(++i).toInt() < size)
                    if (i == hi) break;
                while (size < blockSizes.get(--j).toInt())
                    if (j == lo) break;

                if (i >= j) break;
                swap(blocks, blockSizes, i, j);
            }
            swap(blocks, blockSizes, lo, j);
            return j;
        }

        @Inline
        private static void swap(AddressArray blocks, AddressArray blockSizes, int i, int j) {
            Address temp;
            // blocks
            temp = blocks.get(i);
            blocks.set(i, blocks.get(j));
            blocks.set(j, temp);
            // blockSizes
            temp = blockSizes.get(i);
            blockSizes.set(i, blockSizes.get(j));
            blockSizes.set(j, temp);
        }
    }
}
