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
import org.mmtk.utility.options.EnableLatencyTimer;
import org.mmtk.utility.options.Options;
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
      Word oldValue;
      Address liveWord = getLiveWordAddress(address);
      Word mask = getMask(address);
      if (atomic) {
//        if (set) {
//          do {
//            oldValue = liveWord.prepareWord();
//          } while (!liveWord.attempt(oldValue, oldValue.or(mask)));
//        } else {
//          do {
//            oldValue = liveWord.prepareWord();
//          } while (!liveWord.attempt(oldValue, oldValue.and(mask.not())));
//        }
        do {
          oldValue = liveWord.prepareWord();
        } while (!liveWord.attempt(oldValue, oldValue.or(mask)));
//        do {
//          oldValue = liveWord.prepareWord();
//          newValue = (set) ? oldValue.or(mask) : oldValue.and(mask.not());
//        } while (!liveWord.attempt(oldValue, newValue));
      } else {
        oldValue = liveWord.loadWord();
//        liveWord.store(set ? oldValue.or(mask) : oldValue.and(mask.not()));
        liveWord.store(oldValue.or(mask));
      }
      return oldValue.and(mask).NE(mask);
    }
    @Inline
    protected static boolean liveBitSet(Address address) {
      Address liveWord = getLiveWordAddress(address);
      Word mask = getMask(address);
      Word value = liveWord.loadWord();
      return value.and(mask).EQ(mask);
    }
    @Inline
    private static Word getMask(Address address) {
      int shift = address.toWord().rshl(OBJECT_LIVE_SHIFT).and(WORD_SHIFT_MASK).toInt();
//      Word rtn = Word.one().lsh(shift);
//      return (set) ? rtn : rtn.not();
      return Word.one().lsh(shift);
    }
    static final int METADATA_BYTES = Region.BYTES_IN_REGION * Region.METADATA_REGIONS_PER_CHUNK;
    @Inline
    private static Address getLiveWordAddress(Address address) {
      Address chunk = EmbeddedMetaData.getMetaDataBase(address);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(chunk.NE(Region.of(address)));
      address = address.minus(METADATA_BYTES);
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
      pr = new FreeListPageResource(this, Region.METADATA_PAGES_PER_CHUNK);
    else
      pr = new FreeListPageResource(this, start, extent, Region.METADATA_PAGES_PER_CHUNK);
  }

  public void makeAllocAsMarked() {
    this.allocAsMarked = true;
  }

  @Override
  @Inline
  public void growSpace(Address start, Extent bytes, boolean newChunk) {
    if (newChunk) {
      Address chunk = Conversions.chunkAlign(start.plus(bytes), true);
      HeapLayout.mmapper.ensureMapped(chunk, Region.METADATA_PAGES_PER_CHUNK);
      VM.memory.zero(false, chunk, Extent.fromIntZeroExtend(Region.REGIONS_START_OFFSET));
    }
  }

  @Inline
  public void prepare() {
    // Update mark state
    // Clear marking data
    for (Address b = firstRegion(); !b.isZero(); b = nextRegion(b)) {
      Region.clearMarkBitMap(b);
    }
    // Clear region live size
    for (Address b = firstRegion(); !b.isZero(); b = nextRegion(b)) {
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
   * Return a pointer to a new usable region, or null if none are available.
   *
   * @param allocationKind The generation kind of the result region
   * @return the pointer into the alloc table containing the usable region, {@code null}
   *  if no usable regions are available
   */
  @Inline
  public Address getSpace(int allocationKind) {
    // Allocate
    Address region = acquire(Region.PAGES_IN_REGION);

    if (!region.isZero()) {
      if (VM.VERIFY_ASSERTIONS) {
        Log.write("Region alloc ");
        Log.write(allocationKind == Region.EDEN ? "eden " : (allocationKind == Region.SURVIVOR ? "survivor " : "old "), region);
        Log.writeln(", in chunk ", EmbeddedMetaData.getMetaDataBase(region));
      }

//      if (Region.USE_CARDS) Region.Card.clearCardMetaForRegion(region);

      Region.register(region, allocationKind);

      if (allocationKind != Region.OLD) youngRegions.add(1);
      committedRegions.add(1);
    }

    return region;
  }

  /**
   * Release a region.  A region is free, so call the underlying page allocator
   * to release the associated storage.
   *
   * @param region The address of the Z Page to be released
   */
  @Override
  @Inline
  public void release(Address region) {
    if (VM.VERIFY_ASSERTIONS) Log.writeln("Release region ", region);
    committedRegions.add(-1);
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(committedRegions.get() >= 0);

    if (Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
      youngRegions.add(-1);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(youngRegions.get() >= 0);
    }

    Region.unregister(region);
    if (Region.USE_CARDS) {
      Region.Card.clearCardMetaForRegion(region);
    }

    ((FreeListPageResource) pr).releasePages(region);
  }

  @Inline
  public void initializeHeader(ObjectReference object) {
    if (allocAsMarked) {
      testAndMark(object);
      Region.updateRegionAliveSize(Region.of(object), object);
    } else {
      VM.objectModel.writeAvailableByte(object, (byte) 0);
    }
//    Region.updateRegionAliveSize(Region.of(object), object);
    object.toAddress().store(Word.zero(), VM.objectModel.GC_HEADER_OFFSET());
  }

  @Inline
  public boolean isMarked(ObjectReference object) {
    return MarkBitMap.isMarked(object);
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

//  static final int[][][] markTable; // int[COLLECTORS][REGIONS][BITMAP]

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
      Region.updateRegionAliveSize(region, rtn);
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

  @Inline
  public ObjectReference traceForwardObject(TransitiveClosure trace, ObjectReference object) {
    ObjectReference newObject = ForwardingWord.getForwardedObject(object);
    object = newObject.isNull() ? object : newObject;
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));
    if (testAndMark(object)) {
      trace.processNode(object);
    }
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));
    return object;
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
  public void writeMarkState(ObjectReference object) {
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
    Address region = firstRegion();
    while (!region.isZero()) {
      if (!oldGenerationOnly || Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() == Region.OLD) {
        usedSize += Region.usedSize(region);
        totalRegions++;
      }
      region = nextRegion(region);
    }
    if (totalRegions == 0) return 0;
    return (1f - usedSize / (totalRegions * Region.BYTES_IN_REGION)) * 100;
  }

  // Region iterator

  @Inline
  public Address firstRegion() {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!contiguous);
    return nextRegion(headDiscontiguousRegion);
  }

  @Inline
  public Address nextRegion(Address region) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    int i = region.EQ(chunk) ? 0 : Region.indexOf(region) + 1;
    if (i >= Region.REGIONS_IN_CHUNK) {
      i = 0;
      chunk = getNextChunk(chunk);
    }

    while (true) {
      if (chunk.isZero() || !Space.isMappedAddress(chunk)) return Address.zero();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isMappedAddress(chunk));
      // Get allocated state of the next region in chunk
      Address allocated = chunk.plus(Region.METADATA_OFFSET_IN_CHUNK + i * Region.METADATA_BYTES + Region.METADATA_ALLOCATED_OFFSET);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isMappedAddress(allocated));
      if (allocated.loadByte() != ((byte) 0)) {
        return chunk.plus(Region.REGIONS_START_OFFSET + i * Region.BYTES_IN_REGION);
      }

      i++;
      if (i >= Region.REGIONS_IN_CHUNK) {
        i = 0;
        chunk = getNextChunk(chunk);
      }
    }
  }

  @Inline
  private Address getNextChunk(Address chunk) {
    return HeapLayout.vmMap.getNextContiguousRegion(chunk);
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
        region = firstRegion();
      } else {
        region = nextRegion(region);
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
  public AddressArray snapshotRegions(boolean nurseryOnly) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Plan.gcInProgress());

    int regionsCount = nurseryOnly ? youngRegions() : committedRegions();
    AddressArray regions = AddressArray.create(regionsCount);

    // Initialize regions array
    int index = 0;
    Address region = firstRegion();
    while (!region.isZero()) {
      if (!nurseryOnly || Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
        regions.set(index, region);
        index++;
      }
      if (index >= regions.length()) break;
      region = nextRegion(region);
    }
//    if (VM.VERIFY_ASSERTIONS) {
//      if (regionsCount != index) {
//        Log.write("Invalid iterations: ", index);
//        Log.writeln("/", regionsCount);
//      }
//      VM.assertions._assert(regionsCount == index);
//    }

    return regions;
  }

  /** Include all nursery regions and some old regions */
  @Inline
  public static AddressArray computeRelocationRegions(AddressArray regions, boolean includeAllNursery, boolean concurrent) {
    // Perform relocation set selection
    int regionsCount = regions.length();
    // Initialize regionsSizes array
    AddressArray regionsSizes = AddressArray.create(regionsCount);
    for (int i = 0; i < regionsCount; i++) {
      Address region = regions.get(i);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!region.isZero());
      //if (region.isZero()) continue;
      int size = region.isZero() ? 0 : Region.usedSize(region);
      Address size2 = Address.fromIntZeroExtend(size);
      regionsSizes.set(i, size2);
    }

    // quick sort
    AddressArrayQuickSort.sort(regions, regionsSizes);

    // select relocation regions
    final int BOOT_PAGES = VM.AVAILABLE_START.diff(VM.HEAP_START).toInt() / Constants.BYTES_IN_PAGE;
    int availPages = VM.activePlan.global().getPagesAvail() - BOOT_PAGES;
    int availRegions = ((int) (availPages / EmbeddedMetaData.PAGES_IN_REGION)) * Region.REGIONS_IN_CHUNK;
    int usableBytesForCopying = (int) ((availRegions << Region.LOG_BYTES_IN_REGION) * 0.8);

    int currentSize = 0;
    int relocationRegions = 0;

    // Include all nursery regions
    if (includeAllNursery) {
      for (int i = 0; i < regions.length(); i++) {
        int size = regionsSizes.get(i).toInt();
        Address region = regions.get(i);
        if (Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
          // This is a nursery region
          if (currentSize + size >= usableBytesForCopying) {
            regions.set(i, Address.zero());
            continue;
          }
          currentSize += size;
          relocationRegions++;
        }
      }
    }
    // Include some old regions
    for (int i = 0; i < regions.length(); i++) {
      int size = regionsSizes.get(i).toInt();
      Address region = regions.get(i);
      if (region.isZero()) continue;
      if (includeAllNursery && Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
        // This region is already included
        continue;
      } else if (currentSize + size >= usableBytesForCopying || size > Region.BYTES_IN_REGION * 0.65) {
        regions.set(i, Address.zero());
        continue;
      }
      currentSize += size;
      relocationRegions++;
    }

    // Return relocationSet array
    AddressArray relocationSet = AddressArray.create(relocationRegions);
    int cursor = 0;
    for (int i = 0; i < regions.length(); i++) {
      Address region = regions.get(i);
      if (!region.isZero())
        relocationSet.set(cursor++, region);
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
    public static void sort(AddressArray regions, AddressArray regionSizes) {
      sort(regions, regionSizes, 0, regions.length() - 1);
    }

    @NoInline
    private static void sort(AddressArray regions, AddressArray regionSizes, int lo, int hi) {
      if (hi <= lo) return;
      int j = partition(regions, regionSizes, lo, hi);
      sort(regions, regionSizes, lo, j-1);
      sort(regions, regionSizes, j+1, hi);
    }

    @Inline
    private static int partition(AddressArray regions, AddressArray regionSizes, int lo, int hi) {
      int i = lo, j = hi + 1;
      int size = regionSizes.get(lo).toInt();
      while (true) {
        while (regionSizes.get(++i).toInt() < size)
          if (i == hi) break;
        while (size < regionSizes.get(--j).toInt())
          if (j == lo) break;

        if (i >= j) break;
        swap(regions, regionSizes, i, j);
      }
      swap(regions, regionSizes, lo, j);
      return j;
    }

    @Inline
    private static void swap(AddressArray regions, AddressArray regionSizes, int i, int j) {
      Address temp;
      // regions
      temp = regions.get(i);
      regions.set(i, regions.get(j));
      regions.set(j, temp);
      // regionSizes
      temp = regionSizes.get(i);
      regionSizes.set(i, regionSizes.get(j));
      regionSizes.set(j, temp);
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
  public void cleanupRegions(AddressArray relocationSet, boolean concurrent) {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().rendezvous();
    int regionsToRelease = ceilDiv(relocationSet.length(), workers);

    for (int i = 0; i < regionsToRelease; i++) {
      int cursor = regionsToRelease * id + i;
      if (cursor >= relocationSet.length()) break;
      Address region = relocationSet.get(cursor);
      relocationSet.set(cursor, Address.zero());
      if (!region.isZero()) {
        this.release(region);
      }
    }
    VM.activePlan.collector().rendezvous();
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
    for (Address region = firstRegion(); !region.isZero(); region = nextRegion(region)) {
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
    private static final boolean DEBUG = true;
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
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(word.GT(Word.fromIntZeroExtend(3)));
      object.toAddress().store(word, FORWARDING_POINTER_OFFSET);
    }

    @Inline
    public static void returnToPriorState(ObjectReference object, Word priorStatusWord) {
      object.toAddress().store(priorStatusWord, FORWARDING_POINTER_OFFSET);
    }

    @Inline
    public static Word attemptToForward(ObjectReference object) {
      Word oldValue;
      do {
        oldValue = object.toAddress().plus(FORWARDING_POINTER_OFFSET).loadWord();
        if (!oldValue.and(FORWARDING_MASK).isZero()) return oldValue;
      } while (!attemptForwardingPointer(object, oldValue, oldValue.or(BEING_FORWARDED)));
      return oldValue;
    }

    @Inline
    public static ObjectReference spinAndGetForwardedObject(ObjectReference object, Word statusWord) {
      while (statusWord.and(FORWARDING_MASK).EQ(BEING_FORWARDED))
        statusWord = getForwardingPointer(object);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(statusWord.and(FORWARDING_MASK).EQ(FORWARDED));
      //if (statusWord.and(FORWARDING_MASK).EQ(FORWARDED)) {
      return statusWord.and(FORWARDING_MASK.not()).toAddress().toObjectReference();
//      } else
//        return object;
    }

    public static final Offset OBJECT_END_ADDRESS_OFFSET = VM.objectModel.GC_HEADER_OFFSET().plus(Constants.BYTES_IN_ADDRESS);
    @Inline
    public static void zeroObject(ObjectReference object) {

      Address objectEndAddress = VM.objectModel.getObjectEndAddress(object);
      int size = VM.objectModel.getCurrentSize(object);

      Address start1 = VM.objectModel.objectStartRef(object);
      int headerSize = object.toAddress().diff(start1).toInt();
      Extent extent1 = Extent.fromIntZeroExtend(headerSize + FORWARDING_POINTER_OFFSET.toInt());

      Address start2 = object.toAddress().plus(FORWARDING_POINTER_OFFSET).plus(Constants.BYTES_IN_ADDRESS * 2);
      Extent extent2 = Extent.fromIntZeroExtend(objectEndAddress.diff(start2).toInt());

//      VM.memory.zero(false, start1, extent1);
//      VM.memory.zero(false, start2, extent2);

      object.toAddress().store(objectEndAddress, OBJECT_END_ADDRESS_OFFSET);

      VM.assertions._assert(extent1.toInt() + extent2.toInt() + 8 == size);
//      VM.memory.mprotect(start, size);
    }

    @Inline
    public static ObjectReference forwardObject(ObjectReference object, int allocator) {
      ObjectReference newObject = VM.objectModel.copy(object, allocator);
//      if (DEBUG) zeroObject(object);
      setForwardingPointer(object, newObject.toAddress().toWord().or(FORWARDED));
      return newObject;
    }

    @Inline
    public static ObjectReference forwardObjectWithinMutatorContext(ObjectReference object, int allocator) {
      ObjectReference newObject = VM.objectModel.copyWithinMutatorContext(object, allocator);
//      if (DEBUG) zeroObject(object);
      setForwardingPointer(object, newObject.toAddress().toWord().or(FORWARDED));
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
      return oldObject.toAddress().loadWord(FORWARDING_POINTER_OFFSET).and(FORWARDING_MASK.not()).toAddress().toObjectReference();
    }

    @Inline
    public static ObjectReference getForwardedObjectAtomically(ObjectReference oldObject) {
      Word statusWord = oldObject.toAddress().plus(FORWARDING_POINTER_OFFSET).loadWord();
      if (stateIsForwardedOrBeingForwarded(statusWord)) {
        return ForwardingWord.spinAndGetForwardedObject(oldObject, statusWord);
      } else {
        return oldObject;
      }
    }
  }
}
