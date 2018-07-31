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
import org.mmtk.plan.concurrent.pureg1.PauseTimePredictor;
import org.mmtk.utility.*;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.utility.heap.*;
import org.mmtk.utility.heap.layout.HeapLayout;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.*;

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
public final class RegionSpace extends Space {
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
  private static boolean allocAsMarked = false;
  private static final boolean HEADER_MARK_BIT = false;
  private Lock regionCounterLock = VM.newLock("regionCounterLock");
  public int youngRegions = 0;
  public int committedRegions = 0;

  @Uninterruptible
  private static class Header {
    static byte markState = MARK_BASE_VALUE;
    @Inline
    public static boolean isMarked(ObjectReference object) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((markState & MARK_MASK) == markState);
      return isMarked(VM.objectModel.readAvailableByte(object));
    }
    @Inline
    private static boolean isMarked(byte gcByte) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((markState & MARK_MASK) == markState);
      return ((byte) (gcByte & MARK_MASK)) == markState;
    }
    @Inline
    public static boolean testAndMark(ObjectReference object) {
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
    @Inline
    public static void increaseMarkState() {
      byte rtn = markState;
      do {
        rtn = (byte) (rtn + MARK_INCREMENT);
        rtn &= MARK_MASK;
      } while (rtn < MARK_BASE_VALUE);
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(rtn != markState);
      markState = rtn;
    }
    @Inline
    static void returnToPriorStateAndEnsureUnlogged(ObjectReference object, byte status) {
      if (HeaderByte.NEEDS_UNLOGGED_BIT) status |= HeaderByte.UNLOGGED_BIT;
      VM.objectModel.writeAvailableByte(object, status);
    }
    @Inline
    static void setMarkStateUnlogAndUnlock(ObjectReference object, byte gcByte) {
      byte oldGCByte = gcByte;
      byte newGCByte = (byte) ((oldGCByte & ~MARK_AND_FORWARDING_MASK) | markState);
      if (HeaderByte.NEEDS_UNLOGGED_BIT) newGCByte |= HeaderByte.UNLOGGED_BIT;
      VM.objectModel.writeAvailableByte(object, newGCByte);
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert((oldGCByte & MARK_MASK) != markState);
    }
    @Inline
    public static void writeMarkState(ObjectReference object) {
      byte oldValue = VM.objectModel.readAvailableByte(object);
      byte markValue = markState;
      byte newValue = (byte) (oldValue & ~MARK_AND_FORWARDING_MASK);
      if (HeaderByte.NEEDS_UNLOGGED_BIT)
        newValue |= HeaderByte.UNLOGGED_BIT;
      newValue |= markValue;
      VM.objectModel.writeAvailableByte(object, newValue);
    }
  }

  @Uninterruptible
  private static class MarkBitMap {
    private static final int OBJECT_LIVE_SHIFT = LOG_MIN_ALIGNMENT; // 4 byte resolution
    private static final int LOG_BIT_COVERAGE = OBJECT_LIVE_SHIFT;
    private static final int LOG_LIVE_COVERAGE = LOG_BIT_COVERAGE + LOG_BITS_IN_BYTE;
    private static final Word WORD_SHIFT_MASK = Word.one().lsh(LOG_BITS_IN_WORD).minus(Extent.one());
    @Inline
    public static boolean isMarked(ObjectReference object) {
      return liveBitSet(VM.objectModel.refToAddress(object));
    }
    @Inline
    public static boolean testAndMark(ObjectReference object) {
      return setLiveBit(VM.objectModel.objectStartRef(object), true, true);
    }
    @Inline
    public static void writeMarkState(ObjectReference object) {
      byte oldValue = VM.objectModel.readAvailableByte(object);
      byte newValue = (byte) (oldValue & ~MARK_AND_FORWARDING_MASK);
      if (HeaderByte.NEEDS_UNLOGGED_BIT) newValue |= HeaderByte.UNLOGGED_BIT;
      VM.objectModel.writeAvailableByte(object, newValue);
      setLiveBit(VM.objectModel.objectStartRef(object), true, false);
    }
    @Inline
    private static boolean setLiveBit(Address address, boolean set, boolean atomic) {
      Word oldValue, newValue;
      Address liveWord = getLiveWordAddress(address);
      Word mask = getMask(address, true);
      if (atomic) {
        do {
          oldValue = liveWord.prepareWord();
          newValue = (set) ? oldValue.or(mask) : oldValue.and(mask.not());
        } while (!liveWord.attempt(oldValue, newValue));
      } else {
        oldValue = liveWord.loadWord();
        liveWord.store(set ? oldValue.or(mask) : oldValue.and(mask.not()));
      }
      return oldValue.and(mask).NE(mask);
    }
    @Inline
    protected static boolean liveBitSet(Address address) {
      Address liveWord = getLiveWordAddress(address);
      Word mask = getMask(address, true);
      Word value = liveWord.loadWord();
      return value.and(mask).EQ(mask);
    }
    @Inline
    private static Word getMask(Address address, boolean set) {
      int shift = address.toWord().rshl(OBJECT_LIVE_SHIFT).and(WORD_SHIFT_MASK).toInt();
      Word rtn = Word.one().lsh(shift);
      return (set) ? rtn : rtn.not();
    }
    @Inline
    private static Address getLiveWordAddress(Address address) {
      Address rtn = EmbeddedMetaData.getMetaDataBase(address);
      Address liveWordAddress = rtn.plus(Region.MARKING_METADATA_START).plus(EmbeddedMetaData.getMetaDataOffset(address, LOG_LIVE_COVERAGE, LOG_BYTES_IN_WORD));
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(liveWordAddress.diff(rtn).toInt() < Region.MARKING_METADATA_EXTENT);
      return liveWordAddress;
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
  public RegionSpace(String name, VMRequest vmRequest) {
    this(name, true, VMRequest.discontiguous());
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
  public RegionSpace(String name, boolean zeroed, VMRequest vmRequest) {
    super(name, true, false, zeroed, VMRequest.discontiguous());
    if (vmRequest.isDiscontiguous())
      pr = new FreeListPageResource(this, Region.METADATA_PAGES_PER_REGION);
    else
      pr = new FreeListPageResource(this, start, extent, Region.METADATA_PAGES_PER_REGION);
  }

  public void makeAllocAsMarked() {
    this.allocAsMarked = true;
  }

  @Override
  public void growSpace(Address start, Extent bytes, boolean newChunk) {
    if (newChunk) {
      Address chunk = Conversions.chunkAlign(start.plus(bytes), true);
      if (VM.VERIFY_ASSERTIONS) Log.writeln("New Region ", chunk);
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Conversions.chunkAlign(start.plus(bytes), true).EQ(chunk));
      // MarkBlock.clearRegionMetadata(chunk);
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(chunk.EQ(EmbeddedMetaData.getMetaDataBase(chunk)));
      HeapLayout.mmapper.ensureMapped(chunk, Region.METADATA_PAGES_PER_REGION);
      VM.memory.zero(false, chunk, Extent.fromIntZeroExtend(Region.BLOCKS_START_OFFSET));
    }
  }

  @Inline
  public void prepare(boolean clearBlockLiveSize) {
    prepare();
    if (clearBlockLiveSize) {
      for (Address b = firstBlock(); !b.isZero(); b = nextBlock(b)) {
        Region.setUsedSize(b, 0);
      }
    }
  }
  /**
   * Prepare for a new collection increment.
   */
  @Inline
  private void prepare() {
    if (HEADER_MARK_BIT) {
      Header.increaseMarkState();
      Log.writeln("MarkState ", Header.markState);
    } else {
      // Clear marking data
      for (Address b = firstBlock(); !b.isZero(); b = nextBlock(b)) {
        Region.clearMarkBitMap(b);
      }
    }
  }

  /**
   * A new collection increment has completed.  Release global resources.
   */
  @Inline
  public void release() {}

  /**
   * Return the number of pages reserved for copying.
   */
  @Inline
  public int getCollectionReserve() {
    return 0;
  }

  /**
   * Return the number of pages reserved for use given the pending
   * allocation.  This is <i>exclusive of</i> space reserved for
   * copying.
   */
  @Inline
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
    Address region = acquire(Region.PAGES_IN_BLOCK);

    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.isAligned(region));

    if (VM.VERIFY_ASSERTIONS) {
      Log.write("Block alloc ", region);
      Log.writeln(", in region ", EmbeddedMetaData.getMetaDataBase(region));
    }

    if (!region.isZero()) {
      if (Region.Card.isEnabled()) Region.Card.clearCardMetaForBlock(region);
      //int oldCount = MarkBlock.count();
      //if (MarkBlock.allocated(region)) {
      //  VM.memory.dumpMemory(EmbeddedMetaData.getMetaDataBase(region).plus(MarkBlock.METADATA_OFFSET_IN_REGION), 0, 128);
      //}
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!Region.allocated(region));
      Region.register(region, copy);

      regionCounterLock.acquire();
      youngRegions++;
      committedRegions++;
      regionCounterLock.release();
    }
    /*if (VM.VERIFY_ASSERTIONS) {
      if (!region.isZero()) {
        VM.assertions._assert(Region.allocated(region));
        VM.assertions._assert(!Region.relocationRequired(region));
        VM.assertions._assert(Region.usedSize(region) == 0);
      } else {
        Log.writeln("ALLOCATED A NULL REGION");
      }
    }*/
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
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.isAligned(region));

    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.allocated(region));
    regionCounterLock.acquire();
    committedRegions--;
    if (Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() == 0) {
      youngRegions--;
    }
    regionCounterLock.release();
    Region.unregister(region);

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
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!object.isNull());
    //MarkBlock.setCursor(MarkBlock.of(object.toAddress()), VM.objectModel.getObjectEndAddress(object));
    //VM.assertions._assert(object.toAddress().NE(Address.fromIntZeroExtend(0x692e8c78)));
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Header.isNewObject(object));
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!object.isNull());

    //if (MarkBlock.Card.isEnabled()) MarkBlock.Card.updateCardMeta(object);
    if (allocAsMarked) {
      writeMarkState(object);
      Region.updateBlockAliveSize(Region.of(object), object);
    } else {
      VM.objectModel.writeAvailableByte(object, (byte) 0);
    }

    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
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
    //MarkBlock.setCursor(MarkBlock.of(object.toAddress()), VM.objectModel.getObjectEndAddress(object));
    //VM.assertions._assert(object.toAddress().NE(Address.fromIntZeroExtend(0x692e8c78)));
    writeMarkState(object);
    //if (MarkBlock.Card.isEnabled()) MarkBlock.Card.updateCardMeta(object);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
      if (HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(object));
    }
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
  public ObjectReference traceMarkObject(TransitiveClosure trace, ObjectReference object) {
    ObjectReference rtn = object;

    /*if (ForwardingWord.isForwarded(object)) {
      rtn = getForwardingPointer(object);
    }*/
    /*if (VM.VERIFY_ASSERTIONS) {
      if (ForwardingWord.isForwardedOrBeingForwarded(rtn)) {
        Log.write(rtn);
        Log.write(ForwardingWord.isForwarded(rtn) ? " isForwarded" : " isBeingForwarded");
        Log.writeln(" -> ", getForwardingPointer(rtn));
      }
      VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(rtn));
      //VM.assertions._assert(VM.objectModel.getObjectEndAddress(rtn).LE(MarkBlock.getCursor(MarkBlock.of(rtn.toAddress()))));

    }*/

    if (testAndMark(rtn)) {
      //Log.writeln("Mark ", rtn);
      Address region = Region.of(rtn);
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!rtn.isNull());
      /*_lock.acquire();
      MarkBlock.setUsedSize(region, MarkBlock.usedSize(region) + VM.objectModel.getSizeWhenCopied(rtn));
      _lock.release();*/
      Region.updateBlockAliveSize(region, rtn);
      trace.processNode(rtn);
    }

    // if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
    //if (VM.VERIFY_ASSERTIONS  && HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(rtn));

    //VM.assertions._assert(isLive(rtn));
    return rtn;
  }

  @Inline
  public ObjectReference traceEvacuateObject(TraceLocal trace, ObjectReference object, int allocator, boolean processNode) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));
    if (Region.relocationRequired(Region.of(object))) {
      Word priorStatusWord = ForwardingWord.attemptToForward(object);
      if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
        ObjectReference newObject = ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isLive(newObject));
        return newObject;
      } else {
        long time = VM.statistics.nanoTime();
        ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
        PauseTimePredictor.updateObjectEvacuationTime(newObject, VM.statistics.nanoTime() - time);
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(ForwardingWord.isForwarded(object));
          VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(newObject));
          if (Plan.NEEDS_LOG_BIT_IN_HEADER) VM.assertions._assert(HeaderByte.isUnlogged(newObject));
        }
        if (VM.VERIFY_ASSERTIONS) {
          //Log.write("Forward ", object);
          //Log.writeln(" => ", newObject);
        }
        writeMarkState(newObject);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isLive(newObject));
        trace.processNode(newObject);
        return newObject;
      }
    } else {
      VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
      if (testAndMark(object)) {
        //Log.writeln("EvaMark ", object);
        if (processNode) trace.processNode(object);
      }
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isLive(object));
      return object;
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
    if (ForwardingWord.isForwardedOrBeingForwarded(object)) return true;
    return HEADER_MARK_BIT ? Header.isMarked(object) : MarkBitMap.isMarked(object);
  }

  private void writeMarkState(ObjectReference object) {
    if (HEADER_MARK_BIT)
      Header.writeMarkState(object);
    else
      MarkBitMap.writeMarkState(object);
    // if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isLive(object));
  }

  private static boolean testAndMark(ObjectReference object) {
    return HEADER_MARK_BIT ? Header.testAndMark(object) : MarkBitMap.testAndMark(object);
  }

  public float heapWastePercent(boolean oldGenerationOnly) {
    int usedSize = 0;
    int totalRegions = 0;
    Address block = firstBlock();
    while (!block.isZero()) {
      if (!oldGenerationOnly || Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() != 0) {
        usedSize += Region.usedSize(block);
        totalRegions++;
      }
      block = nextBlock(block);
    }
    if (totalRegions == 0) return 0;
    return (1f - usedSize / (totalRegions * Region.BYTES_IN_BLOCK)) * 100;
  }

  // Block iterator

  @Inline
  public int allocatedRegionsCount() {
    return committedRegions;
  }

  @Inline
  public AddressArray allocatedRegions() {
    AddressArray array = AddressArray.create(allocatedRegionsCount());
    Address region = firstBlock();
    int i = 0;
    do {
      array.set(i, region);
      region = nextBlock(region);
      i += 1;
    } while (!region.isZero());
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(i == allocatedRegionsCount());
    return array;
  }

  @Inline
  public Address firstBlock() {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!contiguous);
    return nextBlock(headDiscontiguousRegion);
  }

  @Inline
  public Address nextBlock(Address block) {
    if (VM.VERIFY_ASSERTIONS) {
      //VM.assertions._assert(!contiguous);
      if (!(block.EQ(EmbeddedMetaData.getMetaDataBase(block)) || (Region.indexOf(block) >= 0 && Region.indexOf(block) < Region.BLOCKS_IN_REGION))) {
        Log.write("Invalid block ", block);
        Log.write(" at region ", EmbeddedMetaData.getMetaDataBase(block));
        Log.write(" with index ", Region.indexOf(block));
        Log.writeln(Region.isAligned(block) ? " aligned" : " not aligned");
      }
      VM.assertions._assert(block.EQ(EmbeddedMetaData.getMetaDataBase(block)) || (Region.indexOf(block) >= 0 && Region.indexOf(block) < Region.BLOCKS_IN_REGION));
    }
    int i = block.EQ(EmbeddedMetaData.getMetaDataBase(block)) ? 0 : Region.indexOf(block) + 1;
    Address region = EmbeddedMetaData.getMetaDataBase(block);
    if (i >= Region.BLOCKS_IN_REGION) {
      i = 0;
      region = getNextRegion(region);//region.plus(NEXT_POINTER_OFFSET_IN_REGION).loadAddress();
    }

    while (true) {
      if (region.isZero()) return Address.zero();

      Address allocated = region.plus(Region.METADATA_OFFSET_IN_REGION + i * Region.METADATA_BYTES + Region.METADATA_ALLOCATED_OFFSET);
      if (allocated.loadByte() != ((byte) 0)) {
        Address rtn = region.plus(Region.BLOCKS_START_OFFSET + i * Region.BYTES_IN_BLOCK);
        return rtn;
      }

      i++;
      if (i >= Region.BLOCKS_IN_REGION) {
        i = 0;
        region = getNextRegion(region);
      }
    }
  }

  @Inline
  private Address getNextRegion(Address region) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!region.isZero());
      VM.assertions._assert(EmbeddedMetaData.getMetaDataBase(region).EQ(region));
    }
    /*if (contiguous) {
      Address nextRegion = region.plus(EmbeddedMetaData.BYTES_IN_REGION);
      Address end = ((FreeListPageResource) pr).getHighWater();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(EmbeddedMetaData.getMetaDataBase(nextRegion).EQ(nextRegion));
      return nextRegion.LT(end) ? nextRegion : Address.zero();
    } else {*/
    return HeapLayout.vmMap.getNextContiguousRegion(region);
    //}
  }

  @Inline
  public AddressArray snapshotBlocks(boolean nurseryOnly) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Plan.gcInProgress());

    int blocksCount = nurseryOnly ? youngRegions : committedRegions;
    if (VM.VERIFY_ASSERTIONS) Log.writeln("Blocks: ", blocksCount);
    AddressArray blocks = AddressArray.create(blocksCount);

    // Initialize blocks array
    int index = 0;
    Address block = firstBlock();
    while (!block.isZero()) {
      //if (VM.VERIFY_ASSERTIONS) {
        //VM.assertions._assert(index < blocks.length());
      //}
      if (!nurseryOnly || Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() == 0) {
        blocks.set(index, block);
        index++;
      }
      block = nextBlock(block);
    }
    if (VM.VERIFY_ASSERTIONS) {
      if (blocksCount != index) {
        Log.write("Invalid iterations: ", index);
        Log.writeln("/", blocksCount);
      }
      VM.assertions._assert(blocksCount == index);
    }

    return blocks;
  }

  @Inline
  public void promoteAllRegionsAsOldGeneration() {
    Address block = firstBlock();
    while (!block.isZero()) {
      Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).store(1);
      block = nextBlock(block);
    }
    regionCounterLock.acquire();
    youngRegions = 0;
    regionCounterLock.release();
  }


  private static Lock relocationSetSelectionLock = VM.newLock("relocation-set-selection-lock");
  private static boolean relocationSetSelected = false;

  @Inline
  public static void prepareComputeRelocationBlocks() {
    relocationSetSelected = false;
  }

  @Inline
  public static AddressArray computeRelocationBlocks(AddressArray blocks, boolean includeAllNursery, boolean concurrent) {

    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!contiguous);
    /*
    int id = concurrent ? VM.activePlan.collector().getId() - VM.activePlan.collector().parallelWorkerCount() : VM.activePlan.collector().getId();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(id >= 0 && id < VM.activePlan.collector().parallelWorkerCount());
    if (id != 0) return null;
    */
/*
    relocationSetSelectionLock.acquire();
    if (relocationSetSelected) {
      relocationSetSelectionLock.release();
      return null;
    }
    relocationSetSelected = true;
    relocationSetSelectionLock.release();
*/
    // Perform relocation set selection
    int blocksCount = blocks.length();
    // Initialize blockSizes array
    AddressArray blockSizes = AddressArray.create(blocksCount);
    for (int i = 0; i < blocksCount; i++) {
      Address block = blocks.get(i);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero());
      //if (block.isZero()) continue;
      int size = block.isZero() ? 0 : Region.usedSize(block);
      Address size2 = Address.fromIntZeroExtend(size);
      blockSizes.set(i, size2);
    }

    // quick sort
    AddressArrayQuickSort.sort(blocks, blockSizes);

    // select relocation blocks
    int availBlocks = ((int) (VM.activePlan.global().getPagesAvail() / EmbeddedMetaData.PAGES_IN_REGION)) * Region.BLOCKS_IN_REGION;
    final int usableBytesForCopying = availBlocks * Region.BYTES_IN_BLOCK;
    int currentSize = 0;
    int relocationBlocks = 0;

    for (int i = 0; i < blocks.length(); i++) {
      int size = blockSizes.get(i).toInt();
      Address block = blocks.get(i);

      if (includeAllNursery && Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() == 0) {
        // Include
      } else if (currentSize + size > usableBytesForCopying || size > Region.BYTES_IN_BLOCK * 0.65) {
        //if (VM.VERIFY_ASSERTIONS) Log.writeln();
        blocks.set(i, Address.zero());
        continue;
      }

      //if (VM.VERIFY_ASSERTIONS) Log.writeln(" relocate");

      currentSize += size;
      //if (!block.isZero())
        //Region.setRelocationState(block, true);
      relocationBlocks++;
    }

    // Return relocationSet array
    AddressArray relocationSet = AddressArray.create(relocationBlocks);
    int cursor = 0;
    for (int i = 0; i < blocks.length(); i++) {
      Address block = blocks.get(i);
      if (!block.isZero())
        relocationSet.set(cursor++, block);
    }

    return relocationSet;
  }

  @Inline
  public static void markRegionsAsRelocate(AddressArray regions) {
    for (int i = 0; i < regions.length(); i++) {
      Address region = regions.get(i);
      if (!region.isZero())
        Region.setRelocationState(region, true);
    }
  }

  @Uninterruptible
  public static class AddressArrayQuickSort {
    @Inline
    public static void sort(AddressArray blocks, AddressArray blockSizes) {
      sort(blocks, blockSizes, 0, blocks.length() - 1);
    }

    private static void sort(AddressArray blocks, AddressArray blockSizes, int lo, int hi) {
      if (hi <= lo) return;
      int j = partition(blocks, blockSizes, lo, hi);
      sort(blocks, blockSizes, lo, j-1);
      sort(blocks, blockSizes, j+1, hi);
    }

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

  @Inline
  private static int ceilDiv(int x, int y) {
    return (x + y - 1) / y;
  }

  @Inline
  public void cleanupBlocks(AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int blocksToRelease = ceilDiv(relocationSet.length(), workers);

    for (int i = 0; i < blocksToRelease; i++) {
      int cursor = blocksToRelease * id + i;
      if (cursor >= relocationSet.length()) break;
      Address block = relocationSet.get(cursor);
      relocationSet.set(cursor, Address.zero());
      if (!block.isZero()) {
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(Region.relocationRequired(block));
          Log.write("Block ", block);
          Log.write(": ", Region.usedSize(block));
          Log.write("/", Region.BYTES_IN_BLOCK);
          Log.writeln(" released");
        }
        if (Region.Card.isEnabled()) {
          Region.Card.clearCardMetaForBlock(block);
          //RemSet.clearRemsetMedaForBlock(block);
        }
        this.release(block);
      }
    }
  }
}
