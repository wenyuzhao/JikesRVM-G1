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
  private static final int  MARK_BASE = ForwardingWord.FORWARDING_BITS;
  private static final int  MAX_MARKCOUNT_BITS = AVAILABLE_LOCAL_BITS - MARK_BASE;
  private static final byte MARK_MASK = (byte) (((1 << MAX_MARKCOUNT_BITS) - 1) << MARK_BASE);
  private static final byte MARK_AND_FORWARDING_MASK = (byte) (MARK_MASK | 3);

  public static final int LOCAL_GC_BITS_REQUIRED = AVAILABLE_LOCAL_BITS;
  public static final int GLOBAL_GC_BITS_REQUIRED = 0;
  public static final int GC_HEADER_WORDS_REQUIRED = 2;
  private static boolean allocAsMarked = false;

  private Atomic.Int youngRegions = new Atomic.Int();
  private Atomic.Int committedRegions = new Atomic.Int();

  @Inline
  public int youngRegions() { return youngRegions.get(); }

  @Inline
  public int committedRegions() { return committedRegions.get(); }

  @Uninterruptible
  private static class MarkBitMap {
    private static final int OBJECT_LIVE_SHIFT = LOG_MIN_ALIGNMENT; // 4 byte resolution
    private static final int LOG_BIT_COVERAGE = OBJECT_LIVE_SHIFT;
    private static final int LOG_LIVE_COVERAGE = LOG_BIT_COVERAGE + LOG_BITS_IN_BYTE;
    private static final Word WORD_SHIFT_MASK = Word.one().lsh(LOG_BITS_IN_WORD).minus(Extent.one());
    @Inline
    public static boolean isMarked(ObjectReference object) {
      return liveBitSet(VM.objectModel.objectStartRef(object));
    }
    @Inline
    public static boolean testAndMark(ObjectReference object) {
      return setLiveBit(VM.objectModel.objectStartRef(object), true, true);
    }
    @Inline
    public static void writeMarkState(ObjectReference object) {
//      byte oldValue = VM.objectModel.readAvailableByte(object);
//      byte newValue = (byte) (oldValue & ~MARK_AND_FORWARDING_MASK);
//      if (HeaderByte.NEEDS_UNLOGGED_BIT) newValue |= HeaderByte.UNLOGGED_BIT;
      VM.objectModel.writeAvailableByte(object, (byte) 0);
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
      Address chunk = EmbeddedMetaData.getMetaDataBase(address);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(chunk.NE(Region.of(address)));
      address = address.minus(Region.BYTES_IN_BLOCK * Region.METADATA_BLOCKS_PER_REGION);
      Address liveWordAddress = chunk.plus(Region.MARKING_METADATA_START).plus(EmbeddedMetaData.getMetaDataOffset(address, LOG_LIVE_COVERAGE, LOG_BYTES_IN_WORD));
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(liveWordAddress.diff(rtn).toInt() < Region.MARKING_METADATA_EXTENT);
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
  @Inline
  public void growSpace(Address start, Extent bytes, boolean newChunk) {
    if (newChunk) {
      Address chunk = Conversions.chunkAlign(start.plus(bytes), true);
      HeapLayout.mmapper.ensureMapped(chunk, Region.METADATA_PAGES_PER_REGION);
      VM.memory.zero(false, chunk, Extent.fromIntZeroExtend(Region.BLOCKS_START_OFFSET));
    }
  }

  @Inline
  public void prepare() {
    // Update mark state
    // Clear marking data
    for (Address b = firstBlock(); !b.isZero(); b = nextBlock(b)) {
      Region.clearMarkBitMap(b);
    }
    // clearBlockLiveSize
    for (Address b = firstBlock(); !b.isZero(); b = nextBlock(b)) {
      Region.setUsedSize(b, 0);
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
   * @param allocationKind The generation kind of the result region
   * @return the pointer into the alloc table containing usable blocks, {@code null}
   *  if no usable blocks are available
   */
  @Inline
  public Address getSpace(int allocationKind) {
    // Allocate
    Address region = acquire(Region.PAGES_IN_BLOCK);

    if (!region.isZero()) {
      if (VM.VERIFY_ASSERTIONS) {
        Log.write("Block alloc ");
        Log.write(allocationKind == Region.EDEN ? "eden " : (allocationKind == Region.SURVIVOR ? "survivor " : "old "), region);
        Log.writeln(", in region ", EmbeddedMetaData.getMetaDataBase(region));
      }

      if (Region.USE_CARDS) Region.Card.clearCardMetaForBlock(region);

      Region.register(region, allocationKind);

      if (allocationKind != Region.OLD) youngRegions.add(1);
      committedRegions.add(1);
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
    committedRegions.add(-1);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(committedRegions.get() >= 0);

    if (Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
      youngRegions.add(-1);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(youngRegions.get() >= 0);
    }

    Region.unregister(region);

    ((FreeListPageResource) pr).releasePages(region);
  }

  @Inline
  public void initializeHeader(ObjectReference object) {
    if (allocAsMarked) {
      writeMarkState(object);
      Region.updateBlockAliveSize(Region.of(object), object);
    } else {
      VM.objectModel.writeAvailableByte(object, (byte) 0);
    }
    object.toAddress().store(Word.zero(), VM.objectModel.GC_HEADER_OFFSET());
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
    initializeHeader(object);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(object.toAddress().loadWord(VM.objectModel.GC_HEADER_OFFSET()).isZero());
//    } else {
//      VM.objectModel.writeAvailableByte(object, (byte) 0);
//    }

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
    initializeHeader(object);
    if (VM.VERIFY_ASSERTIONS) {
      Word state = object.toAddress().loadWord(VM.objectModel.GC_HEADER_OFFSET());
      if (!state.isZero()) {
        Log.writeln("Invalid state ", state);
        VM.assertions.fail("");
      }
    }
    //if (MarkBlock.Card.isEnabled()) MarkBlock.Card.updateCardMeta(object);
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
//      if (HeaderByte.NEEDS_UNLOGGED_BIT) VM.assertions._assert(HeaderByte.isUnlogged(object));
//    }
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

//    if (VM.VERIFY_ASSERTIONS) {
//      if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
//        VM.objectModel.dumpObject(object);
//      }
//      VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
//    }

    if (testAndMark(rtn)) {
      Address region = Region.of(rtn);
      Region.updateBlockAliveSize(region, rtn);
      trace.processNode(rtn);
    }

    return rtn;
  }

  @Uninterruptible
  static public abstract class EvacuationTimer {
    @Inline
    public abstract void updateObjectEvacuationTime(ObjectReference ref, long time);
  }

  @Inline
  public ObjectReference traceEvacuateObject(TraceLocal trace, ObjectReference object, int allocator, EvacuationTimer evacuationTimer) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));
    if (Region.relocationRequired(Region.of(object))) {
      Word priorStatusWord = ForwardingWord.attemptToForward(object);

      if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
        ObjectReference newObject = ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
        return newObject;
      } else {
        long time = VM.statistics.nanoTime();
        ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
        if (evacuationTimer != null) {
          evacuationTimer.updateObjectEvacuationTime(newObject, VM.statistics.nanoTime() - time);
        }
        trace.processNode(newObject);
        return newObject;
      }
    } else {
      if (testAndMark(object)) {
        trace.processNode(object);
      }
      return object;
    }
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
    return MarkBitMap.isMarked(object);
  }

  @Inline
  private void writeMarkState(ObjectReference object) {
    MarkBitMap.writeMarkState(object);
  }

  @Inline
  private static boolean testAndMark(ObjectReference object) {
    return MarkBitMap.testAndMark(object);
  }

  @Inline
  public float heapWastePercent(boolean oldGenerationOnly) {
    int usedSize = 0;
    int totalRegions = 0;
    Address block = firstBlock();
    while (!block.isZero()) {
      if (!oldGenerationOnly || Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() == Region.OLD) {
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
  public Address firstBlock() {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!contiguous);
    return nextBlock(headDiscontiguousRegion);
  }

  @Inline
  public Address nextBlock(Address block) {
//    if (VM.VERIFY_ASSERTIONS) {
//      //VM.assertions._assert(!contiguous);
//      if (!(block.EQ(EmbeddedMetaData.getMetaDataBase(block)) || (Region.indexOf(block) >= 0 && Region.indexOf(block) < Region.BLOCKS_IN_REGION))) {
//        Log.write("Invalid block ", block);
//        Log.write(" at region ", EmbeddedMetaData.getMetaDataBase(block));
//        Log.write(" with index ", Region.indexOf(block));
//        Log.writeln(Region.isAligned(block) ? " aligned" : " not aligned");
//      }
//      VM.assertions._assert(block.EQ(EmbeddedMetaData.getMetaDataBase(block)) || (Region.indexOf(block) >= 0 && Region.indexOf(block) < Region.BLOCKS_IN_REGION));
//    }
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
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(!region.isZero());
//      VM.assertions._assert(EmbeddedMetaData.getMetaDataBase(region).EQ(region));
//    }
    /*if (contiguous) {
      Address nextRegion = region.plus(EmbeddedMetaData.BYTES_IN_REGION);
      Address end = ((FreeListPageResource) pr).getHighWater();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(EmbeddedMetaData.getMetaDataBase(nextRegion).EQ(nextRegion));
      return nextRegion.LT(end) ? nextRegion : Address.zero();
    } else {*/
    return HeapLayout.vmMap.getNextContiguousRegion(region);
    //}
  }

  @Uninterruptible
  public class RegionIterator {
    Address region = Address.zero();
    Lock lock = VM.newLock("RegionIteratorLock");
    boolean completed = false;
    @Inline
    public void reset() {
      region = Address.zero();
      completed = false;
    }
    @Inline
    public Address next() {
      lock.acquire();
      if (completed) {
        region = Address.zero();
      } else if (region.isZero()) {
        region = firstBlock();
      } else {
        region = nextBlock(region);
      }
      if (region.isZero()) {
        completed = true;
      }
      Address rtn = region;
      lock.release();
      return rtn;
    }
  }

  public final RegionIterator regionIterator = new RegionIterator();

  @Inline
  public AddressArray snapshotBlocks(boolean nurseryOnly) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Plan.gcInProgress());

    int blocksCount = nurseryOnly ? youngRegions() : committedRegions();
    AddressArray blocks = AddressArray.create(blocksCount);

    // Initialize blocks array
    int index = 0;
    Address block = firstBlock();
    while (!block.isZero()) {
      if (!nurseryOnly || Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
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
      Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).store(Region.OLD);
      block = nextBlock(block);
    }
    youngRegions.set(0);
  }


  private static Lock relocationSetSelectionLock = VM.newLock("relocation-set-selection-lock");
  private static boolean relocationSetSelected = false;

  @Inline
  public static void prepareComputeRelocationBlocks() {
    relocationSetSelected = false;
  }

  /** Include all nursery regions and some old regions */
  @Inline
  public static AddressArray computeRelocationBlocks(AddressArray blocks, boolean includeAllNursery, boolean concurrent) {
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
    int usableBytesForCopying = (int) (availBlocks * Region.BYTES_IN_BLOCK * 0.9);

    int currentSize = 0;
    int relocationBlocks = 0;

    // Include all nursery regions
    if (includeAllNursery) {
      for (int i = 0; i < blocks.length(); i++) {
        int size = blockSizes.get(i).toInt();
        Address region = blocks.get(i);
        if (Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
          // This is a nursery region
          if (currentSize + size >= usableBytesForCopying) {
            blocks.set(i, Address.zero());
            continue;
          }
          currentSize += size;
          relocationBlocks++;
        }
      }
    }
    // Include some old regions
    for (int i = 0; i < blocks.length(); i++) {
      int size = blockSizes.get(i).toInt();
      Address block = blocks.get(i);
      if (block.isZero()) continue;
      if (includeAllNursery && Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
        // This region is already included
        continue;
      } else if (currentSize + size >= usableBytesForCopying || size > Region.BYTES_IN_BLOCK * 0.65) {
        blocks.set(i, Address.zero());
        continue;
      }
      currentSize += size;
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
      if (!region.isZero()) {
        Region.setRelocationState(region, true);
      }
    }
  }

  @Uninterruptible
  public static class AddressArrayQuickSort {
    @Inline
    public static void sort(AddressArray blocks, AddressArray blockSizes) {
      sort(blocks, blockSizes, 0, blocks.length() - 1);
    }

    @Inline
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
  public boolean contains(Address address) {
    return !address.isZero() && Space.isInSpace(descriptor, address) && Region.allocated(Region.of(address));
  }

  @Inline
  public boolean contains(ObjectReference ref) {
    return contains(VM.objectModel.objectStartRef(ref));
  }

  @Inline
  private static int ceilDiv(int x, int y) {
    return (x + y - 1) / y;
  }

  @Inline
  public void releaseZeroRegions(AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    if (concurrent) id -= workers;
    int blocksToRelease = ceilDiv(relocationSet.length(), workers);

    for (int i = 0; i < blocksToRelease; i++) {
      int cursor = blocksToRelease * id + i;
      if (cursor >= relocationSet.length()) break;
      Address block = relocationSet.get(cursor);
      if (block.isZero() || Region.usedSize(block) != 0) {
        continue;
      }
      relocationSet.set(cursor, Address.zero());
      if (!block.isZero()) {
        if (Region.USE_CARDS) {
          Region.Card.clearCardMetaForBlock(block);
          RemSet.removeRemsetForRegion(this, block);
        }
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(Region.relocationRequired(block));
          Log.write("Block ", block);
          Log.write(": ", Region.usedSize(block));
          Log.write("/", Region.BYTES_IN_BLOCK);
          Log.writeln(" released");
        }
        this.release(block);
      }
    }
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
        if (Region.USE_CARDS) {
          Region.Card.clearCardMetaForBlock(block);
        }
        this.release(block);
      }
    }
  }

  /**
   * Heap Validation Methods
   */

  private final TransitiveClosure regionValidationTransitiveClosure = new TransitiveClosure() {
    @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
      ObjectReference ref = slot.loadObjectReference();
      if (!VM.debugging.validRef(ref)) {
        Log.writeln();
        Log.write("Invalid ", source);
        Log.write(".", slot);
        Log.writeln(" -> ", ref);
      }
      VM.assertions._assert(VM.debugging.validRef(ref));
      if (Region.USE_CARDS && contains(ref)) {
        Address region = Region.of(ref);
        if (region.NE(Region.of(source))) {
          Address card = Region.Card.of(source);
          VM.assertions._assert(RemSet.contains(region, card));
        }
      }
    }
  };

  private final LinearScan regionValidationLinearScan = new LinearScan() {
    @Uninterruptible public void scan(ObjectReference object) {
      if ((VM.objectModel.readAvailableByte(object) & 3) == 1) return;
      VM.assertions._assert(VM.debugging.validRef(object));
      VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
      VM.scanning.scanObject(regionValidationTransitiveClosure, object);
    }
  };

  public void validate() {
    for (Address region = firstBlock(); !region.isZero(); region = nextBlock(region)) {
      Region.linearScan(regionValidationLinearScan, region);
    }
  }

  @Uninterruptible
  public final static class ForwardingWord {
    private static final Word FORWARDING_NOT_TRIGGERED_YET = Word.zero(); // ...00
    private static final Word BEING_FORWARDED = Word.fromIntZeroExtend(2); // ...10
    private static final Word FORWARDED =       Word.fromIntZeroExtend(3); // ...11
    public static final Word FORWARDING_MASK =  Word.fromIntZeroExtend(3); // ...11
    public static final int FORWARDING_BITS = 2;
    private static final Offset FORWARDING_POINTER_OFFSET = VM.objectModel.GC_HEADER_OFFSET();

    static {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!FORWARDING_POINTER_OFFSET.isZero());
    }

    @Inline
    private static Word getForwardingPointer(ObjectReference object) {
      return object.toAddress().loadWord(FORWARDING_POINTER_OFFSET);
    }

    @Inline
    private static boolean attemptForwardingPointer(ObjectReference object, Word oldValue, Word newValue) {
      return object.toAddress().plus(FORWARDING_POINTER_OFFSET).attempt(oldValue, newValue);
    }

    @Inline
    private static void setForwardingPointer(ObjectReference object, Word word) {
      VM.assertions._assert(word.GT(Word.fromIntZeroExtend(3)));
      object.toAddress().store(word, FORWARDING_POINTER_OFFSET);
    }

    @Inline
    public static Word attemptToForward(ObjectReference object) {
      Word oldValue;
      do {
        oldValue = object.toAddress().plus(FORWARDING_POINTER_OFFSET).loadWord();
        if (!oldValue.and(FORWARDING_MASK).isZero()) {
          if (!(oldValue.EQ(BEING_FORWARDED) || oldValue.GT(FORWARDED))) {
            VM.objectModel.dumpObject(object);
            Log.writeln("Invalid status ", oldValue);
            Log.writeln("FORWARDING_POINTER_OFFSET ", FORWARDING_POINTER_OFFSET);
          }
          VM.assertions._assert(oldValue.EQ(BEING_FORWARDED) || oldValue.GT(FORWARDED));
          return oldValue;
        }
      } while (!attemptForwardingPointer(object, oldValue, oldValue.or(BEING_FORWARDED)));
      return oldValue;
    }

    @Inline
    public static ObjectReference spinAndGetForwardedObject(ObjectReference object, Word statusWord) {
      Word initialStatus = getForwardingPointer(object);
      while (statusWord.and(FORWARDING_MASK).EQ(BEING_FORWARDED))
        statusWord = getForwardingPointer(object);
      if (statusWord.and(FORWARDING_MASK).EQ(FORWARDED)) {
        ObjectReference newObj = statusWord.and(FORWARDING_MASK.not()).toAddress().toObjectReference();
        if (newObj.isNull()) {
          VM.objectModel.dumpObject(object);
          Log.write(initialStatus);
          Log.write(initialStatus);
          Log.writeln(" ", getForwardingPointer(object));
        }
        VM.assertions._assert(!newObj.isNull());
        VM.assertions._assert(VM.debugging.validRef(newObj));
        return newObj;
      } else
        return object;
    }

    @Inline
    public static ObjectReference forwardObject(ObjectReference object, int allocator) {
      ObjectReference newObject = VM.objectModel.copy(object, allocator);
      setForwardingPointer(object, newObject.toAddress().toWord().or(FORWARDED));
      ObjectReference newObj = getForwardingPointer(object).and(FORWARDING_MASK.not()).toAddress().toObjectReference();;
      VM.assertions._assert(!newObj.isNull());
      VM.assertions._assert(VM.debugging.validRef(newObj));
      VM.assertions._assert(!newObject.isNull());
      VM.assertions._assert(VM.debugging.validRef(newObject));
      return newObject;
    }

    @Inline
    public static boolean isForwarded(ObjectReference object) {
      return getForwardingPointer(object).and(FORWARDING_MASK).EQ(FORWARDED);
    }

    @Inline
    public static boolean isForwardedOrBeingForwarded(ObjectReference object) {
      return !getForwardingPointer(object).and(FORWARDING_MASK).isZero();
    }

    @Inline
    public static boolean stateIsForwardedOrBeingForwarded(Word header) {
      return !header.and(FORWARDING_MASK).isZero();
    }

    @Inline
    public static ObjectReference getForwardedObject(ObjectReference oldObject) {
      return getForwardingPointer(oldObject).and(FORWARDING_MASK.not()).toAddress().toObjectReference();
    }
  }
}
