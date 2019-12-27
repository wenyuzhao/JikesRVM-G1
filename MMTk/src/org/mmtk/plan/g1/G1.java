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
package org.mmtk.plan.g1;

import org.mmtk.plan.*;
import org.mmtk.policy.region.CardTable;
import org.mmtk.policy.region.CollectionSet;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.RegionSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.deque.SharedDeque;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.options.*;
import org.mmtk.utility.sanitychecker.SanityChecker;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.pragma.Unpreemptible;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

/**
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class G1 extends G1Base {

  public static final boolean VERBOSE = false;

  public static final RegionSpace regionSpace = new RegionSpace("g1");
  public static final int REGION_SPACE = regionSpace.getDescriptor();
  public static final int ALLOC_G1_EDEN     = Plan.ALLOCATORS + 1;
  public static final int ALLOC_G1_SURVIVOR = Plan.ALLOCATORS + 2;
  public static final int ALLOC_G1_OLD      = Plan.ALLOCATORS + 3;
  public static final int SCAN_MARK = 0;
  public static final int SCAN_EVACUATE = 1;
  public static final int SCAN_NURSERY = 2;
  public static final int SCAN_VALIDATE = 3;
  // GC Kinds
  public static boolean inGC = false;
  public static int gcKind = GCKind.YOUNG;
  public static class GCKind {
    public static final int YOUNG = 0;
    public static final int MIXED = 1;
    public static final int FULL = 2;
  }
  public static final PauseTimePredictor predictor = new PauseTimePredictor();

  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace evacuateTrace = new Trace(metaDataSpace);
  public final Trace nurseryTrace = new Trace(metaDataSpace);
  public final SharedDeque modbufPool = new SharedDeque("modBufs", metaDataSpace, 1);
  protected boolean inConcurrentCollection = false;

  //public static boolean concurrentMarkingInProgress = false;

  static {
    Options.g1ReservePercent = new G1ReservePercent();
    Options.g1InitiatingHeapOccupancyPercent = new G1InitiatingHeapOccupancyPercent();
    Options.g1GCLiveThresholdPercent = new G1GCLiveThresholdPercent();
    Options.g1MaxNewSizePercent = new G1MaxNewSizePercent();
    Options.g1NewSizePercent = new G1NewSizePercent();
    Options.g1HeapWastePercent = new G1HeapWastePercent();
  }


  /**
   * Constructor
   */
  public G1() {
    collection = -1;
  }

  @Override
  @Interruptible("Spawning collector threads requires allocation")
  protected void spawnCollectorThreads(int numThreads) {
    if (ENABLE_CONCURRENT_REFINEMENT) {
      ConcurrentRefinementWorker.spawn();
//      ConcurrentRefinementWorker.GROUP.initGroup(1, ConcurrentRefinementWorker.class);
    }
    super.spawnCollectorThreads(numThreads);
  }

  @Override
  @Interruptible
  public void processOptions() {
    super.processOptions();
    if (ENABLE_CONCURRENT_MARKING) {
      int oldClosure = Phase.scheduleCollector(CLOSURE);
      int newClosure = Phase.scheduleComplex(concurrentClosure);
      ComplexPhase cp = (ComplexPhase) Phase.getPhase(mixedCollection);
      cp.replacePhase(oldClosure, newClosure);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void collectionPhase(short phaseId) {
    if (VERBOSE) {
      Log.write("Global ");
      Log.writeln(Phase.getName(phaseId));
    }

    if (phaseId == SET_COLLECTION_KIND) inGC = true;
    if (phaseId == COMPLETE) inGC = false;
//
    if (phaseId == SET_BARRIER_ACTIVE) {
      G1Mutator.newMutatorBarrierActive = true;
      return;
    }

    if (phaseId == CLEAR_BARRIER_ACTIVE) {
      G1Mutator.newMutatorBarrierActive = false;
      return;
    }

    if (phaseId == PREPARE) {
      if (ENABLE_CONCURRENT_MARKING) {
        inConcurrentCollection = true;
        modbufPool.reset();
        modbufPool.prepareNonBlocking();
      }
      loSpace.prepare(true);
      immortalSpace.prepare();
      VM.memory.globalPrepareVMSpace();
      regionSpace.clearNextMarkTables();
      regionSpace.prepare();
      markTrace.prepareNonBlocking();
      return;
    }

    if (phaseId == CLOSURE) {
      return;
    }

    if (phaseId == RELEASE) {
      if (ENABLE_CONCURRENT_MARKING) {
        inConcurrentCollection = false;
        modbufPool.reset();//(1);
      }
      markTrace.release();
      if (!ENABLE_REMEMBERED_SETS) {
        VM.memory.globalReleaseVMSpace();
        loSpace.release(true);
        immortalSpace.release();
      }
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION) {
//      if (gcKind == GCKind.YOUNG) Log.writeln("Young GC");
//      if (gcKind == GCKind.MIXED) Log.writeln("Mixed GC");
//      if (gcKind == GCKind.FULL)  Log.writeln("Full GC");
      if (ENABLE_CONCURRENT_REFINEMENT) ConcurrentRefinementWorker.pause();
//      Space.printVMMap();
      predictor.prepare();
      int availablePages = getTotalPages() - getPagesUsed();
      CollectionSet.compute(regionSpace, gcKind, availablePages, predictor);
      return;
    }

    if (phaseId == REFINE_CARDS) {
      return;
    }

    if (phaseId == EVACUATE_PREPARE) {
      regionSpace.shiftMarkTables();
      if (!ENABLE_REMEMBERED_SETS) {
        VM.memory.globalPrepareVMSpace();
        immortalSpace.prepare();
        loSpace.prepare(true);
        regionSpace.prepare();
      } else {
        regionSpace.resetAllocRegions();
      }
      (gcKind == GCKind.YOUNG ? nurseryTrace : evacuateTrace).prepare();
      return;
    }

    if (phaseId == EVACUATE_CLOSURE) {
      return;
    }

    if (phaseId == EVACUATE_RELEASE) {
      regionSpace.clearRemSetCardsPointingToCollectionSet();
      (gcKind == GCKind.YOUNG ? nurseryTrace : evacuateTrace).release();
      if (G1.gcKind == GCKind.YOUNG) {
        regionSpace.release();
      } else {
        loSpace.release(true);
        immortalSpace.release();
        VM.memory.globalReleaseVMSpace();
        regionSpace.release();
      }
      if (ENABLE_CONCURRENT_REFINEMENT) ConcurrentRefinementWorker.resume();
      return;
    }

    if (phaseId == COMPLETE) {
      predictor.release(gcKind == GCKind.YOUNG);
    }

    if (phaseId == Validation.VALIDATE_PREPARE) {
      Space.printVMMap();
      Validation.prepare();
      return;
    }

    if (phaseId == Validation.VALIDATE_RELEASE) {
      Validation.release();
      return;
    }

    super.collectionPhase(phaseId);
  }

  private static final float G1_INITIATING_HEAP_OCCUPANCY_PERCENT = VM.activePlan.constraints().g1InitiatingHeapOccupancy();

  @Override
  protected boolean concurrentCollectionRequired() {
    if (!ENABLE_CONCURRENT_MARKING) return false;
    if (!Phase.concurrentPhaseActive()) {
      if (regionSpace.committedRatio() > G1_INITIATING_HEAP_OCCUPANCY_PERCENT) {
        gcKind = GCKind.MIXED;
        return true;
      }
    }
    return false;
  }

  @Override
  protected boolean collectionRequired(boolean spaceFull, Space space) {
    boolean heapFull = getPagesReserved() > getTotalPages();
    if (spaceFull || heapFull) {
      gcKind = GCKind.FULL;
      return true;
    }
    if (ENABLE_GENERATIONAL_GC && !inGC) {
      if (regionSpace.nurseryRatio() > predictor.nurseryRatio) {
        gcKind = GCKind.YOUNG;
        return true;
      }
    }
    return false;
  }

  @Override
  public void forceFullHeapCollection() {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!Plan.gcInProgress());
    gcKind = GCKind.FULL;
  }

  @Inline
  @Unpreemptible
  @Override
  public void prepareForUserCollectionRequest() {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!Plan.gcInProgress());
    gcKind = GCKind.FULL;
  }


  /**
   * Return the number of pages reserved for copying.
   */
  @Override
  public final int getCollectionReserve() {
    return regionSpace.reservedPages() / 10;
  }

  @Override
  public int sanityExpectedRC(ObjectReference object, int sanityRootRC) {
    Space space = Space.getSpaceForObject(object);
    // Nursery
    if (space == regionSpace) {
      // We are never sure about objects in RS.
      // This is not very satisfying but allows us to use the sanity checker to
      // detect dangling pointers.
      return SanityChecker.UNSURE;
    }
    return super.sanityExpectedRC(object, sanityRootRC);
  }

  @Override
  @Inline
  public int getPagesUsed() {
    return (int) (regionSpace.reservedPages() + super.getPagesUsed());
  }

  @Override
  public boolean willNeverMove(ObjectReference object) {
    return Space.isInSpace(REGION_SPACE, object) ? false : true;
  }

  @Override
  @Interruptible
  protected void registerSpecializedMethods() {
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, G1MarkTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_EVACUATE, G1EvacuateTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_NURSERY, G1NurseryTraceLocal.class);
//    TransitiveClosure.registerSpecializedScan(SCAN_VALIDATE, Validation.TraceLocal.class);
    super.registerSpecializedMethods();
  }

  final static Word LOG_MASK = Word.one().lsh(2);
  final protected static byte LOG_BIT = 1 << 2;

  @Inline
  public static void markAsLogged(ObjectReference o) {
    byte value = VM.objectModel.readAvailableByte(o);
    VM.objectModel.writeAvailableByte(o, (byte) (value | LOG_BIT));
  }

  @Inline
  public static boolean isUnlogged(ObjectReference o) {
    byte value = VM.objectModel.readAvailableByte(o);
    return (value & LOG_BIT) == 0;
  }

  @Inline
  public static boolean attemptLog(ObjectReference o) {
    Word oldValue, newValue;
    do {
      oldValue = VM.objectModel.prepareAvailableBits(o);
      if (!oldValue.and(LOG_MASK).isZero()) return false;
      newValue = oldValue.or(LOG_MASK);
    } while (!VM.objectModel.attemptAvailableBits(o, oldValue, newValue));
    return true;
  }

  @Inline
  public static boolean attemptUnlog(ObjectReference o) {
    Word oldValue, newValue;
    do {
      oldValue = VM.objectModel.prepareAvailableBits(o);
      if (oldValue.and(LOG_MASK).isZero()) return false;
      newValue = oldValue.and(LOG_MASK.not());
    } while (!VM.objectModel.attemptAvailableBits(o, oldValue, newValue));
    return true;
  }

  @Inline
  public static int pickCopyAllocator(ObjectReference o) {
    if (!ENABLE_GENERATIONAL_GC) return ALLOC_G1_OLD;
    switch (Region.getInt(Region.of(o), Region.MD_GENERATION)) {
      case Region.EDEN: return G1.ALLOC_G1_SURVIVOR;
      default:          return G1.ALLOC_G1_OLD;
    }
  }
}
