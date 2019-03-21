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
//import org.mmtk.utility.;
import org.mmtk.utility.HeaderByte;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.Conversions;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
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
  public static final int LOCAL_GC_BITS_REQUIRED = AVAILABLE_LOCAL_BITS;
  public static final int GLOBAL_GC_BITS_REQUIRED = 0;
  public static final int GC_HEADER_WORDS_REQUIRED = 1;
  private static boolean allocAsMarked = true;

  private Atomic.Int youngRegions = new Atomic.Int();
  private Atomic.Int committedRegions = new Atomic.Int();

  @Inline
  public int youngRegions() { return youngRegions.get(); }

  @Inline
  public int committedRegions() { return committedRegions.get(); }

  @Uninterruptible
  private static class MarkBitMap {
    @Inline
    public static boolean isMarked(ObjectReference object) {
      return liveBitSet(VM.objectModel.refToAddress(object));
    }
    @Inline
    public static boolean testAndMark(ObjectReference object) {
      return setLiveBit(VM.objectModel.refToAddress(object), true);
    }
    @Inline
    public static boolean writeMarkState(ObjectReference object) {
      return setLiveBit(VM.objectModel.refToAddress(object), false);
    }
    @Inline
    private static boolean setLiveBit(Address address, boolean atomic) {
      Word oldValue;
      Address liveWord = getLiveWordAddress(address);
      Word mask = getMask(address);
      if (atomic) {
        do {
          oldValue = liveWord.prepareWord();
          if (oldValue.or(mask).EQ(oldValue)) return false;
        } while (!liveWord.attempt(oldValue, oldValue.or(mask)));
      } else {
        oldValue = liveWord.loadWord();
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

    private static final Word WORD_SHIFT_MASK = Word.fromIntZeroExtend(BITS_IN_WORD).minus(Word.one());
    private static final int LOG_LIVE_COVERAGE = 2 + LOG_BITS_IN_BYTE;

    @Inline
    private static Word getMask(Address address) {
      int shift = address.toWord().rshl(2).and(WORD_SHIFT_MASK).toInt();
      return Word.one().lsh(shift);
    }
    @Inline
    private static Address getLiveWordAddress(Address address) {
      Address chunk = EmbeddedMetaData.getMetaDataBase(address);
      Address rtn = chunk.plus(EmbeddedMetaData.getMetaDataOffset(address, LOG_LIVE_COVERAGE, LOG_BYTES_IN_WORD));
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(rtn.LT(chunk.plus(Region.BYTES_IN_MARKTABLE)));
        Address region = Region.of(address);
        int index = Region.indexOf(region) + 1;
        Address start = chunk.plus(index * Region.MARK_BYTES_PER_REGION);
        VM.assertions._assert(rtn.GE(start));
        Address end = start.plus(Region.MARK_BYTES_PER_REGION);
        VM.assertions._assert(rtn.LT(end));
      }
      return rtn;
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
    ((FreeListPageResource) pr).pageOffsetLogAlign = Region.LOG_PAGES_IN_REGION;
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
      VM.memory.zero(false, chunk, Extent.fromIntZeroExtend(Region.METADATA_PAGES_PER_CHUNK << LOG_BYTES_IN_PAGE));
    }
  }

  public void prepare() {
    prepare(false);
  }

  public void clearCSetMarkMap() {
    Address r;
    regionIterator.reset();
    while (!(r = regionIterator.next()).isZero()) {
      if (Region.relocationRequired(r))
        Region.clearMarkBitMapForRegion(r);
    }
  }

  @NoInline
  public void prepare(boolean nursery) {
    // Update mark state
    // Clear marking data
    Address r;
    regionIterator.reset();
    while (!(r = regionIterator.next()).isZero()) {
      Region.clearMarkBitMapForRegion(r);
      Region.setUsedSize(r, 0);
      // Set TAMS
      if (!nursery) {
        Address cursor = Region.metaDataOf(r, Region.METADATA_CURSOR_OFFSET).loadAddress();
//        Log.write("region ", r);
//        Log.writeln(" cursor ", cursor);
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(cursor.GE(r));
          if (!cursor.LE(r.plus(Region.BYTES_IN_REGION))) {
            Log.write("Invalid cursor ", cursor);
            Log.write( " region ", r);
            Log.writeln( " end ", r.plus(Region.BYTES_IN_REGION));
          }
          VM.assertions._assert(cursor.LE(r.plus(Region.BYTES_IN_REGION)));
        }
        Region.metaDataOf(r, Region.METADATA_TAMS_OFFSET).store(cursor);
      }
    }
//    resetTLABs(1);
  }

  /**
   * A new collection increment has completed.  Release global resources.
   */
//  @NoInline
  public void release() {
//    Address r;
//    regionIterator.reset();
//    while (!(r = regionIterator.next()).isZero()) {
//      // Set TAMS
//      Address slot = Region.metaDataOf(r, Region.METADATA_TAMS_OFFSET);
//      if (!nursery) {
//        Address cursor = Region.metaDataOf(r, Region.METADATA_CURSOR_OFFSET).loadAddress();
//        if (VM.VERIFY_ASSERTIONS) {
//          VM.assertions._assert(TAMS.GE(cursor));
//          VM.assertions._assert(TAMS.LT(cursor.plus(Region.BYTES_IN_REGION)));
//        }
//        Region.metaDataOf(r, Region.METADATA_TAMS_OFFSET).store(cursor);
//      }
//    }
    resetTLABs(0);
  }

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

  Lock tlabLock = VM.newLock("tlab-lock-m");

  Atomic.Int tlabLock2 = new Atomic.Int();
//  int[] tlabLock3 = new int[] { 0 };
//  Lock tlabLockCollector = VM.newLock("tlab-lock-c");
//  Lock tlabCollectorLock = VM.newLock("tlab-lock-collector");
  AddressArray allocTLABs = AddressArray.create(3);
//  AddressArray allocRegions = AddressArray.create(3);
//  AddressArray allocTLABs = AddressArray.create(3);
//  WordArray allocTLABIndex = WordArray.create(3);

  @Inline
  private void lock() {
    do {
      if (VM.VERIFY_ASSERTIONS) {
        int oldValue = tlabLock2.get();
        VM.assertions._assert(oldValue == 0 || oldValue == 1);
      }
    } while (!tlabLock2.attempt(0, 1));
  }

  @Inline
  private void unlock() {
    tlabLock2.set(0);
  }

  @Inline
  public void resetTLABs(int start) {
    for (int i = start; i < 3; i++) {
      allocTLABs.set(i, Address.zero());
    }
  }

//  public Address tryAllocTLAB(int allocationKind, int tlabs) {
//    Address slot = ObjectReference.fromObject(allocTLABs).toAddress().plus(allocationKind << LOG_BYTES_IN_ADDRESS);
//    Address oldTLAB = slot.prepareAddress();
//    Address newTLAB = oldTLAB.plus(tlabs << Region.LOG_BYTES_IN_TLAB);
//    boolean newRegionAllocated = false;
//    if (oldTLAB.isZero() || Region.of(newTLAB).NE(Region.of(oldTLAB))) {
//      Address region = getSpace(allocationKind);
//      if (region.isZero()) return Address.zero();
//      newTLAB = region.plus(tlabs << Region.LOG_BYTES_IN_TLAB);
//      newRegionAllocated = true;
//    }
//    boolean success = slot.attempt(oldTLAB, newTLAB);
//    if (!success) {
//      if (newRegionAllocated) release(Region.of(newTLAB));
//      return tryAllocTLAB(allocationKind, tlabs);
//    }
//    return newRegionAllocated ? Region.of(newTLAB) : oldTLAB;
//  }
  @Inline
  public Address allocTLABFast(int allocationKind, int tlabSize0) {
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(tlabSize0 >= Region.BYTES_IN_TLAB);
//      VM.assertions._assert(tlabSize0 <= Region.BYTES_IN_REGION);
//    }
    final Address slot = ObjectReference.fromObject(allocTLABs).toAddress().plus(allocationKind << LOG_BYTES_IN_ADDRESS);
    Extent tlabSize = Extent.fromIntZeroExtend(tlabSize0);
    Word oldCursor, newCursor;
    do {
      oldCursor = slot.prepareWord();
      if (oldCursor.isZero()) return Address.zero();
      newCursor = oldCursor.plus(tlabSize);
      if (!newCursor.xor(oldCursor).rshl(Region.LOG_BYTES_IN_REGION).isZero()) {
        if (newCursor.and(Region.REGION_MASK).isZero()) {
          newCursor = Word.zero();
        } else {
          return Address.zero();
        }
      }
    } while (!slot.attempt(oldCursor, newCursor));
    return oldCursor.toAddress();
  }

  public Address allocTLABSlow(int allocationKind, int tlabSize) {
//    lock();
//    Address result = allocTLABFast(allocationKind, tlabSize);
//    if (!result.isZero()) {
//      unlock();
//      return result;
//    }
//    Address region = getSpace(allocationKind);
//    if (region.isZero()) {
//      unlock();
//      return region;
//    }
//    Address newCursor = region.plus(tlabSize);
//    if (tlabSize == Region.BYTES_IN_REGION) newCursor = Address.zero();
//    final Address slot = ObjectReference.fromObject(allocTLABs).toAddress().plus(allocationKind << LOG_BYTES_IN_ADDRESS);
//    slot.store(newCursor);
//    unlock();
    final Address slot = ObjectReference.fromObject(allocTLABs).toAddress().plus(allocationKind << LOG_BYTES_IN_ADDRESS);
    Address oldCursor = slot.prepareAddress();
    Address region = getSpace(allocationKind);
    if (region.isZero()) return region;
    if (tlabSize == Region.BYTES_IN_REGION) {
      return region;
    }
    Address newCursor = region.plus(tlabSize);
    slot.store(newCursor);
    return region;
//    if (slot.attempt(oldCursor, newCursor)) {
//      return region;
//    } else {
//      release(region);
//      return allocTLAB(allocationKind, tlabSize);
//    }
  }

  @LogicallyUninterruptible
  @Override
  public Address acquire(int pages) {
    boolean allowPoll = VM.activePlan.isMutator() && Plan.isInitialized();

    /* Check page budget */
    int pagesReserved = pr.reservePages(pages);

    /* Poll, either fixing budget or requiring GC */
    if (allowPoll && VM.activePlan.global().poll(false, this)) {
      pr.clearRequest(pagesReserved);
//      Log.writeln("Block for GC");
      unlock();
      VM.collection.blockForGC();
      lock();
      return Address.zero(); // GC required, return failure
    }

    /* Page budget is ok, try to acquire virtual memory */
    Address rtn = pr.getNewPages(pagesReserved, pages, zeroed);
    if (rtn.isZero()) {
      /* Failed, so force a GC */
      if (!allowPoll) VM.assertions.fail("Physical allocation failed when polling not allowed!");
      boolean gcPerformed = VM.activePlan.global().poll(true, this);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(gcPerformed, "GC not performed when forced.");
      pr.clearRequest(pagesReserved);
//      Log.writeln("Block for GC");
//      tlabLock.release();
      unlock();
      VM.collection.blockForGC();
//      tlabLock.acquire();
      lock();
      return Address.zero();
    }

    return rtn;
  }

  @Inline
  public Address allocTLAB(int allocationKind, int tlabSize) {
//    final Address slot = ObjectReference.fromObject(allocTLABs).toAddress().plus(allocationKind << LOG_BYTES_IN_ADDRESS);
    Address tlab = allocTLABFast(allocationKind, tlabSize);
    if (!tlab.isZero()) {
      return tlab;
    }
    return allocTLABSlow(allocationKind, tlabSize);
  }

  /**
   * Return a pointer to a new usable region, or null if none are available.
   *
   * @param allocationKind The generation kind of the result region
   * @return the pointer into the alloc table containing the usable region, {@code null}
   *  if no usable regions are available
   */
  public Address getSpace(int allocationKind) {
    // Allocate
//    Log.writeln("Acquire start");
//    if (concurrent)
    Address region = super.acquire(Region.PAGES_IN_REGION);
//    Log.writeln("Acquire end");
    if (VM.VERIFY_ASSERTIONS) {
      if (!Region.isAligned(region)) {
        Log.writeln("Unaligned region ", region);
      }
      VM.assertions._assert(Region.isAligned(region));
    }

    if (!region.isZero()) {
//      Log.writeln(Region.verbose() ? "Region verbose" : "Region not verbose");
      if (Region.verbose()) {
        Log.write("Region alloc ");
        Log.write(allocationKind == Region.EDEN ? "eden " : (allocationKind == Region.SURVIVOR ? "survivor " : "old "), region);
        Log.writeln(", in chunk ", EmbeddedMetaData.getMetaDataBase(region));
      }

//      if (Region.USE_CARDS) Region.Card.clearCardMetaForRegion(region);
      insertRegion(region);
      Region.register(region, allocationKind);
//      if (!slowPath) {
        if (allocationKind != Region.OLD) youngRegions.add(1);
        committedRegions.add(1);
//      } else {
//        if (allocationKind != Region.OLD) youngRegions.addNonAtomic(1);
//        committedRegions.addNonAtomic(1);
//      }
    } else {
      if (Region.verbose()) {
        Log.writeln("Region allocation failure");
      }
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
  @NoInline
  public void release(Address region) {
    if (Region.verbose()) Log.writeln("Release region ", region);
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
    removeRegion(region);
    ((FreeListPageResource) pr).releasePages(region);
  }

  @Inline
  public void initializeHeader(ObjectReference object, int size) {
//    if (allocAsMarked) {
//      testAndMark(object);
//      if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
//      Region.updateRegionAliveSize(Region.of(object), object);
//    } else {
//      VM.objectModel.writeAvailableByte(object, (byte) 0);
//    }
//    Region.updateRegionAliveSize(Region.of(object), object);
//    object.toAddress().store(Word.zero(), VM.objectModel.GC_HEADER_OFFSET());
  }

  @Inline
  public void postCopy(ObjectReference object, int size) {
//    testAndMark(object);
//    if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
    object.toAddress().store(Word.zero(), VM.objectModel.GC_HEADER_OFFSET());
//    ForwardingWord.clearForwardingBits(object);
    if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
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
//    ObjectReference rtn = object;

//    if (VM.VERIFY_ASSERTIONS) {
//      if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
//        VM.objectModel.dumpObject(object);
//      }
//      VM.assertions._assert(!ForwardingWord.isForwardedOrBeingForwarded(object));
//    }

    if (testAndMark(object)) {
      if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.attemptLog(object);
//      Log.writeln("Mark ", object);
      Address region = Region.of(object);
      Region.updateRegionAliveSize(region, object);
      trace.processNode(object);
//      return object;
    }
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(VM.debugging.validRef(object));
//      VM.assertions._assert(isLive(object));
//    }
    return object;
  }

  @Uninterruptible
  static public abstract class EvacuationTimer {
    @Inline
    public abstract void updateObjectEvacuationTime(long size, long time);
  }

  @Uninterruptible
  static public abstract class EvacuationAccumulator {
    @Inline
    public abstract void updateObjectEvacuationTime(long size, long time);
  }

  @Inline
  public ObjectReference traceEvacuateCSetObject(TraceLocal trace, ObjectReference object, int allocator, EvacuationAccumulator evacuationTimer) {
    if (Region.relocationRequired(Region.of(object))) {
      Word priorStatusWord = ForwardingWord.attemptToForward(object);

      if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
        return ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
      } else {
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(VM.debugging.validRef(object));
        }
        long time = VM.statistics.nanoTime();
        ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
        evacuationTimer.updateObjectEvacuationTime(VM.objectModel.getSizeWhenCopied(newObject), VM.statistics.nanoTime() - time);
        trace.processNode(newObject);
        return newObject;
      }
    } else {
//      if (testAndMark(object))
//        trace.processNode(object);
      return object;
    }
  }

  @Inline
  public ObjectReference traceForwardCSetObject(TransitiveClosure trace, ObjectReference object) {
//    if (Region.relocationRequired(Region.of(object)))
    if (!ForwardingWord.isForwarded(object)) {
//      if (VM.VERIFY_ASSERTIONS)
//        VM.assertions._assert(!Region.relocationRequired(Region.of(object)));
      return object;
    }
    if (VM.VERIFY_ASSERTIONS)
      VM.assertions._assert(Region.relocationRequired(Region.of(object)));
//    Word status = VM.objectModel.readAvailableBitsWord(object);
    ObjectReference ref = ForwardingWord.getForwardedObject(object);

    if (testAndMark(ref)) trace.processNode(ref);
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(ref));
    return ref;
//    ObjectReference newObject = ForwardingWord.getForwardedObject(object);
//    object = newObject.isNull() ? object : newObject;
//    return object;
  }

  @Inline
  public ObjectReference traceEvacuateObject(TraceLocal trace, ObjectReference object, int allocator, EvacuationAccumulator evacuationTimer) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));
//    final Address region = Region.of(object);
//    if (!Region.relocationRequired(region)) return
    if (Region.relocationRequired(Region.of(object))) {
      Word priorStatusWord = ForwardingWord.attemptToForward(object);

      if (ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
        ObjectReference newObject = ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
//        if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
        return newObject;
      } else {
        long time = VM.statistics.nanoTime();
        ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
        if (evacuationTimer != null) {
          evacuationTimer.updateObjectEvacuationTime(VM.objectModel.getSizeWhenCopied(newObject), VM.statistics.nanoTime() - time);
        }
        trace.processNode(newObject);
//        if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
        return newObject;
      }
    } else {
      if (testAndMark(object)) {
//        if (HeaderByte.NEEDS_UNLOGGED_BIT) HeaderByte.markAsLogged(object);
        trace.processNode(object);
      }
      return object;
    }
  }

  @Inline
  public ObjectReference traceForwardObject(TransitiveClosure trace, ObjectReference object) {
    if (Region.relocationRequired(Region.of(object))) {
//      Word status = VM.objectModel.readAvailableBitsWord(object);
      object = ForwardingWord.getForwardedObject(object);
//
//      if (VM.VERIFY_ASSERTIONS) {
//        VM.assertions._assert(!object.isNull());
//      }
    }

//    ObjectReference newObject = ForwardingWord.getForwardedObject(object);
//    if (VM.VERIFY_ASSERTIONS) {
//      if (Region.relocationRequired(Region.of(object))) {
//        if (newObject.isNull())
//          Log.writeln("Unforwarded object ", object);
//        VM.assertions._assert(!newObject.isNull());
//        VM.assertions._assert(newObject.toAddress().NE(object.toAddress()));
//      }
//    }
//    object = newObject.isNull() ? object : newObject;
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));
    if (testAndMark(object)) {
//      HeaderByte.markAsLogged(object);
      trace.processNode(object);
    }
//    if (HeaderByte.isUnlogged(object)) {
//      VM.assertions.fail("Redirected object set is unlogged");
//    }
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
    if (ForwardingWord.isForwarded(object)) return true;
    Address region = Region.of(object);
    Address TAMS = Region.metaDataOf(region, Region.METADATA_TAMS_OFFSET).loadAddress();
    if (VM.VERIFY_ASSERTIONS) {
      if (TAMS.LT(region)) {
        Log.write("Invalid tams ", TAMS);
        Log.writeln(" region ", region);
        VM.objectModel.dumpObject(object);
      }
      VM.assertions._assert(TAMS.GE(region));
      VM.assertions._assert(TAMS.LE(region.plus(Region.BYTES_IN_REGION)));
    }
    if (VM.objectModel.objectStartRef(object).GE(TAMS)) return true;
//    Address tams = Region.metaDataOf(Region.of(object), Region.METADATA_TAMS_OFFSET).loadAddress();
//    if (VM.objectModel.refToAddress(object).GE(tams)) {
//      return true;
//    }
    return isMarked(object);
  }

  @Inline
  public boolean isAfterTAMS(ObjectReference object) {
    Address region = Region.of(object);
    Address TAMS = Region.metaDataOf(region, Region.METADATA_TAMS_OFFSET).loadAddress();
    return VM.objectModel.objectStartRef(object).GE(TAMS);
  }

  @Inline
  public boolean isMarked(ObjectReference object) {
    return MarkBitMap.isMarked(object);
  }

  @Inline
  private static boolean testAndMark(ObjectReference object) {
    return MarkBitMap.testAndMark(object);
  }

  @Inline
  private static boolean writeMarkState(ObjectReference object) {
    return MarkBitMap.writeMarkState(object);
  }

//  @Inline
//  public float heapWastePercent(boolean oldGenerationOnly) {
//    int usedSize = 0;
//    int totalRegions = 0;
//    Address region = firstRegion();
//    while (!region.isZero()) {
//      if (!oldGenerationOnly || Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() == Region.OLD) {
//        usedSize += Region.usedSize(region);
//        totalRegions++;
//      }
//      region = nextRegion(region);
//    }
//    if (totalRegions == 0) return 0;
//    return (1f - usedSize / (totalRegions * Region.BYTES_IN_REGION)) * 100;
//  }

  // Region iterator

//  @Inline
//  public Address firstRegion() {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!contiguous);
//    return nextRegion(headDiscontiguousRegion);
//  }

//  @Inline
//  public Address nextRegion(Address region) {
//    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
//    int i = region.EQ(chunk) ? 0 : Region.indexOf(region) + 1;
//    if (i >= Region.REGIONS_IN_CHUNK) {
//      i = 0;
//      chunk = getNextChunk(chunk);
//    }
////HeapLayout.vmMap.getContiguousRegionChunks()
//    while (true) {
//      if (chunk.isZero() || !Space.isMappedAddress(chunk)) return Address.zero();
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isMappedAddress(chunk));
//      // Get allocated state of the next region in chunk
//      Address allocated = chunk.plus(Region.METADATA_OFFSET_IN_CHUNK + i * Region.METADATA_BYTES + Region.METADATA_ALLOCATED_OFFSET);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isMappedAddress(allocated));
//      if (allocated.loadByte() != ((byte) 0)) {
//        return chunk.plus(Region.REGIONS_START_OFFSET + i * Region.BYTES_IN_REGION);
//      }
//
//      i++;
//      if (i >= Region.REGIONS_IN_CHUNK) {
//        i = 0;
//        chunk = getNextChunk(chunk);
//      }
//    }
//  }

//  @Inline
//  private Address getNextChunk(Address chunk) {
//    return HeapLayout.vmMap.getNextContiguousRegion(chunk);
//  }

//  final Lock regionsLock = VM.newLock("regions-lock");
//  final byte[] regions = new byte[VM.HEAP_END.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION];
  final AddressArray regions = AddressArray.create(VM.HEAP_END.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION);
  @Inline
  private void insertRegion(Address region) {
//    regionsLock.acquire();
//    Address ptr = ObjectReference.fromObject(regions).toAddress();
//    int index = region.diff(VM.HEAP_START).toInt() >> Region.LOG_BYTES_IN_REGION;
//    ptr.plus(index).store(1);
    final int index = region.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION;
    if (VM.VERIFY_ASSERTIONS) {
      if (index < 0 || index >= regions.length()) {
        Log.writeln("invalid region: ", region);
        VM.assertions.fail("ArrayIndexOutOfBoundsException");
      }
    }
    regions.set(index, region);
//    regionsLock.release();
  }
  @Inline
  private void removeRegion(Address region) {
//    Address ptr = ObjectReference.fromObject(regions).toAddress();
//    int index = region.diff(VM.HEAP_START).toInt() >> Region.LOG_BYTES_IN_REGION;
//    ptr.plus(index).store(0);
//    regionsLock.acquire();
    regions.set(region.diff(VM.HEAP_START).toInt() >>> Region.LOG_BYTES_IN_REGION, Address.zero());
//    regionsLock.release();
  }

  int cursor = 0;

  @Inline
  public void resetRegionIterator() {
    cursor = 0;
  }

  @Inline
  public Address nextRegion() {
//      Address ptr = ObjectReference.fromObject(regions).toAddress();
    final int len = regions.length();
    while (cursor < len) {
      Address region = regions.get(cursor);
      cursor += 1;
      if (!region.isZero()) {
        return region; //VM.HEAP_START.plus(cursor << Region.LOG_BYTES_IN_REGION);
      }
    }
    return Address.zero();


//      if (region.isZero()) return Address.zero();
//      // move `region` to next allocated G1 region
//      while (!region.isZero() && (EmbeddedMetaData.getMetaDataBase(region).EQ(region) || !Region.allocated(region))) {
//        region = region.plus(Region.BYTES_IN_REGION);
//        if (EmbeddedMetaData.getMetaDataBase(region).EQ(region)) {
//          // Bump to next chunk
//          chunkCursor += 1;
//          region = chunk = chunk.plus(EmbeddedMetaData.BYTES_IN_REGION);
//          if (chunkCursor >= chunkLimit) {
//            // next contiguous region
//            region = chunk = contiguousRegion = HeapLayout.vmMap.getNextContiguousRegion(contiguousRegion);
//            chunkCursor = 0;
//            chunkLimit = contiguousRegion.isZero() ? 0 : HeapLayout.vmMap.getContiguousRegionChunks(contiguousRegion);
//          }
//        }
//      }
//      if (VM.VERIFY_ASSERTIONS) {
//        VM.assertions._assert(region.isZero() || Space.isMappedAddress(region));
//        VM.assertions._assert(region.isZero() || Space.isInSpace(getDescriptor(), region));
//        VM.assertions._assert(region.isZero() || Region.allocated(region));
//      }
//      Address result = region;
//      region = region.plus(Region.BYTES_IN_REGION);
//      return result;

//      Address result = region;
//      // Skip first metadata region
//      if (EmbeddedMetaData.getMetaDataBase(region).EQ(region)) {
//        result = region = region.plus(Region.BYTES_IN_REGION);
//      }
//
//      // update next region
//      region = region.plus(Region.BYTES_IN_REGION);
//      if (EmbeddedMetaData.getMetaDataBase(region).EQ(region) || Region.indexOf(region) >= Region.REGIONS_IN_CHUNK) {
//        chunkCursor += 1;
//        region = chunk = chunk.plus(EmbeddedMetaData.BYTES_IN_REGION);
//        if (chunkCursor >= chunkLimit) {
//          region = chunk = contiguousRegion = HeapLayout.vmMap.getNextContiguousRegion(contiguousRegion);
//          chunkCursor = 0;
//          chunkLimit = contiguousRegion.isZero() ? 0 : HeapLayout.vmMap.getContiguousRegionChunks(contiguousRegion);
//        }
//      }
//      return result;
  }

  @Uninterruptible
  public class RegionIterator {
    @Inline
    public void reset() {
      resetRegionIterator();
    }

    @Inline
    public Address next() {
      return nextRegion();
    }
  }

  public final RegionIterator regionIterator = new RegionIterator();

//  @NoInline
//  public int calculateRemSetPages() {
//      int remsetPages = 0;
//      int fixedPages = 0;
//      regionIterator.reset();
//      Address region;
//      while (!(region = regionIterator.next()).isZero()) {
//        fixedPages += 1;
//        remsetPages += Region.metaDataOf(region, Region.METADATA_REMSET_PAGES_OFFSET).loadInt();
////          remsetCards += Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
//      }
//      return (remsetPages / 16) + fixedPages;
//  }

  @NoInline
  public int calculateRemSetCards() {
    int remsetCards = 0;
    regionIterator.reset();
    Address region;
    while (!(region = regionIterator.next()).isZero()) {
      if (Region.allocated(region))
        remsetCards += Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
    }
    return remsetCards;
  }

//  @Inline
  public AddressArray snapshotRegions(boolean nurseryOnly) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Plan.gcInProgress());

    int regionsCount = nurseryOnly ? youngRegions() : committedRegions();
    AddressArray regions = AddressArray.create(regionsCount);

    // Initialize regions array
    int index = 0;
    regionIterator.reset();
    Address region = regionIterator.next();
    while (!region.isZero()) {
      if (!nurseryOnly || Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt() != Region.OLD) {
        regions.set(index, region);
        index++;
      }
      if (index >= regions.length()) break;
      region = regionIterator.next();//(region);
    }

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
      if (!region.isZero()) {
        int size = Region.usedSize(region);
//        size += delta;
        regionsSizes.set(i, Address.fromIntZeroExtend(size));
      } else {
        regionsSizes.set(i, Address.zero());
      }
    }

    // quick sort
    AddressArrayQuickSort.sort(regions, regionsSizes);

    // select relocation regions
//    final int BOOT_PAGES = VM.AVAILABLE_START.diff(VM.HEAP_START).toInt() / Constants.BYTES_IN_PAGE;
    int availPages = VM.activePlan.global().getPagesAvail();// - BOOT_PAGES;
    int availRegions = ((int) (availPages / EmbeddedMetaData.PAGES_IN_REGION)) * Region.REGIONS_IN_CHUNK;
    int usableBytesForCopying = (int) (availRegions << Region.LOG_BYTES_IN_REGION);

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
      if (!region.isZero()) {
//        if (VM.VERIFY_ASSERTIONS) Log.writeln("Relocate ", region);
        relocationSet.set(cursor++, region);
      }
    }
//    if (VM.VERIFY_ASSERTIONS) Log.writeln("Relocation set size = ", cursor);

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
    return contains(VM.objectModel.refToAddress(ref));
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
//        if (Region.verbose()) Log.writeln("Release ", region);
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

//  public void validate() {
//    VM.assertions.fail("Unimplemented");
//    for (Address region = firstRegion(); !region.isZero(); region = nextRegion(region)) {
//      Region.linearScan(regionValidationLinearScan, region);
//    }
//  }

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

//    public static final Offset OBJECT_END_ADDRESS_OFFSET = VM.objectModel.GC_HEADER_OFFSET().plus(Constants.BYTES_IN_ADDRESS);
//    @Inline
//    public static void zeroObject(ObjectReference object) {
//
//      Address objectEndAddress = VM.objectModel.getObjectEndAddress(object);
//      int size = VM.objectModel.getCurrentSize(object);
//
//      Address start1 = VM.objectModel.objectStartRef(object);
//      int headerSize = object.toAddress().diff(start1).toInt();
//      Extent extent1 = Extent.fromIntZeroExtend(headerSize + FORWARDING_POINTER_OFFSET.toInt());
//
//      Address start2 = object.toAddress().plus(FORWARDING_POINTER_OFFSET).plus(Constants.BYTES_IN_ADDRESS * 2);
//      Extent extent2 = Extent.fromIntZeroExtend(objectEndAddress.diff(start2).toInt());
//
////      VM.memory.zero(false, start1, extent1);
////      VM.memory.zero(false, start2, extent2);
//
//      object.toAddress().store(objectEndAddress, OBJECT_END_ADDRESS_OFFSET);
//
//      VM.assertions._assert(extent1.toInt() + extent2.toInt() + 8 == size);
////      VM.memory.mprotect(start, size);
//    }

    @Inline
    public static ObjectReference forwardObject(ObjectReference object, int allocator) {
//      if (HeaderByte.isUnlogged(object)) {
//        VM.assertions.fail("Object in collection set is unlogged");
//      }
      ObjectReference newObject = VM.objectModel.copy(object, allocator);
//      if (DEBUG) zeroObject(object);
      setForwardingPointer(object, newObject.toAddress().toWord().or(FORWARDED));
//      if (HeaderByte.isUnlogged(newObject)) {
//        VM.assertions.fail("Forwarded object is unlogged");
//      }
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
