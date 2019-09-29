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
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

/**
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class G1 extends G1Base {

  public static final boolean VERBOSE = false;

  public static final RegionSpace regionSpace = new RegionSpace("region", VMRequest.discontiguous());
  public static final int REGION_SPACE = regionSpace.getDescriptor();
  public static final int ALLOC_G1 = Plan.ALLOC_DEFAULT;
  public static final int SCAN_MARK = 0;
  public static final int SCAN_EVACUATE = 1;

  public final Trace markTrace = new Trace(metaDataSpace);
  public final Trace evacuateTrace = new Trace(metaDataSpace);
  public static AddressArray blocksSnapshot, relocationSet;
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
    regionSpace.makeAllocAsMarked();
    smallCodeSpace.makeAllocAsMarked();
    nonMovingSpace.makeAllocAsMarked();
  }


  /**
   * Constructor
   */
  public G1() {
    collection = _collection;
  }

  @Override
  @Interruptible
  public void processOptions() {
    super.processOptions();
    if (ENABLE_CONCURRENT_MARKING) {
      replacePhase(Phase.scheduleCollector(CLOSURE), Phase.scheduleComplex(concurrentClosure));
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
      Log.writeln( ENABLE_CONCURRENT_MARKING ? "ENABLE_CONCURRENT_MARKING=1" : "ENABLE_CONCURRENT_MARKING=0");
    }
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
      loSpace.release(true);
      immortalSpace.release();
      VM.memory.globalReleaseVMSpace();
      return;
    }

    if (phaseId == RELOCATION_SET_SELECTION) {
      AddressArray blocksSnapshot = regionSpace.snapshotRegions(false);
      relocationSet = RegionSpace.computeRelocationRegions(blocksSnapshot, false, false);
      RegionSpace.markRegionsAsRelocate(relocationSet);
      return;
    }

    if (phaseId == EVACUATE_PREPARE) {
      loSpace.prepare(true);
      immortalSpace.prepare();
      VM.memory.globalPrepareVMSpace();
      regionSpace.shiftMarkTables();
      regionSpace.prepare();
      evacuateTrace.prepare();
      return;
    }

    if (phaseId == EVACUATE_CLOSURE) {
      return;
    }

    if (phaseId == EVACUATE_RELEASE) {
      evacuateTrace.release();
      regionSpace.release();
      loSpace.release(true);
      immortalSpace.release();
      VM.memory.globalReleaseVMSpace();
      return;
    }

    super.collectionPhase(phaseId);
  }

  /****************************************************************************
   *
   * Accounting
   */

  final int BOOT_PAGES = VM.AVAILABLE_START.diff(VM.HEAP_START).toInt() / Constants.BYTES_IN_PAGE;
  final float RESERVE_PERCENT = Options.g1ReservePercent.getValue() / 100f;


//  protected boolean collectionRequired(boolean spaceFull, Space space) {
//    Log.writeln(spaceFull ? "spaceFull=1" : "spaceFull=0");
//    boolean stressForceGC = stressTestGCRequired();
//    boolean heapFull = getPagesReserved() > getTotalPages();
//    Log.writeln("getPagesReserved ", getPagesReserved());
//    Log.writeln("getTotalPages ", getTotalPages());
//    Log.writeln(heapFull ? "heapFull=1" : "heapFull=0");
//
//    return spaceFull || stressForceGC || heapFull;
//  }

  @Override
  protected boolean concurrentCollectionRequired() {
    if (!ENABLE_CONCURRENT_MARKING) return false;
    boolean x = !Phase.concurrentPhaseActive() &&
        ((getPagesReserved() * 100) / getTotalPages()) > 45;
    if (x) {
//      Log.writeln("[CONC MARK]");
    }
    return x;
  }

  @Override
  protected boolean collectionRequired(boolean spaceFull, Space space) {
    final int totalPages = getTotalPages();
    final boolean heapFull = ((totalPages - getPagesReserved()) * 10) < totalPages;
    return spaceFull || heapFull;
  }


  /**
   * Return the number of pages reserved for copying.
   */
  @Override
  public final int getCollectionReserve() {
    // we must account for the number of pages required for copying,
    // which equals the number of semi-space pages reserved
    return 0;//regionSpace.reservedPages() / 10;
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
    return super.getPagesUsed() + regionSpace.reservedPages();
  }

  @Override
  public boolean willNeverMove(ObjectReference object) {
    if (Space.isInSpace(REGION_SPACE, object)) return false;
    return super.willNeverMove(object);
  }

  @Override
  @Interruptible
  protected void registerSpecializedMethods() {
    TransitiveClosure.registerSpecializedScan(SCAN_MARK, G1MarkTraceLocal.class);
    TransitiveClosure.registerSpecializedScan(SCAN_EVACUATE, G1EvacuateTraceLocal.class);
    super.registerSpecializedMethods();
  }

  final static Word LOG_MASK = Word.one().lsh(2);

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
}
