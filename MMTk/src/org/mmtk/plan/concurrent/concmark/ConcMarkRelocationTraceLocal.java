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
package org.mmtk.plan.concurrent.concmark;

import org.mmtk.plan.Phase;
import org.mmtk.plan.Plan;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.MarkRegion;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements the core functionality for a transitive
 * closure over the heap graph.
 */
@Uninterruptible
public class ConcMarkRelocationTraceLocal extends TraceLocal {

  public ConcMarkRelocationTraceLocal(Trace trace) {
    super(ConcMark.SCAN_RELOCATE, trace);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(ConcMark.CONC_MARK, object))
      return ConcMark.markRegionSpace.isLive(object);
    return super.isLive(object);
  }

  @Override
  public void prepare() {
    super.prepare();
    Log.writeln("Memory: " + VM.activePlan.global().getPagesUsed() + " / " + VM.activePlan.global().getTotalPages());
    Log.writeln("BLOCK SIZE " + MarkRegion.count());
    int aliveSizeInRelocationSet = 0;
    int useableBytesForCopying = (int) (VM.activePlan.global().getPagesAvail() * (1.0 - MarkRegion.METADATA_PAGES_PER_MMTK_REGION / EmbeddedMetaData.PAGES_IN_REGION) * Constants.BYTES_IN_PAGE);

    for (Address region : MarkRegion.iterate()) {
      Log.write("#Block " + region + ": " + MarkRegion.usedSize(region) + "/" + MarkRegion.BYTES_IN_REGION);
      int usedSize = MarkRegion.usedSize(region);
      if (usedSize <= (MarkRegion.BYTES_IN_REGION >> 1)) {
        if (aliveSizeInRelocationSet + usedSize <= useableBytesForCopying) {
          Log.write(" relocate");
          MarkRegion.setRelocationState(region, true);
          aliveSizeInRelocationSet += usedSize;
        }
      }
      Log.writeln();
    };
  }

  static Lock lock = VM.newLock("RelocationGlobal");

  @Override
  public void release() {
    super.release();
    lock.acquire();
    int visitedPages = 0;
    for (Address region : MarkRegion.iterate()) {
      if (MarkRegion.relocationRequired(region)) {
        Log.writeln("#Block " + region + ": " + MarkRegion.usedSize(region) + "/" + MarkRegion.BYTES_IN_REGION + " released");
        MarkRegion.setRelocationState(region, false);
        ConcMark.markRegionSpace.release(region);
      } else {
        visitedPages++;
        Log.writeln("#Block " + region + ": " + MarkRegion.usedSize(region) + "/" + MarkRegion.BYTES_IN_REGION);
      }
    }
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(visitedPages == MarkRegion.count(), "Invalid iteration, only " + visitedPages + "/" + MarkRegion.count() + " blocks are iterated");
    lock.release();
    Log.writeln("Memory: " + VM.activePlan.global().getPagesReserved() + " / " + VM.activePlan.global().getTotalPages() + ", " + ConcMark.markRegionSpace.availablePhysicalPages());
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!Phase.concurrentPhaseActive());
    if (object.isNull()) return object;
    if (Space.isInSpace(ConcMark.CONC_MARK, object))
      return ConcMark.markRegionSpace.traceRelocateObject(this, object, ConcMark.ALLOC_Z);
    return super.traceObject(object);
  }

  /**
   * Will this object move from this point on, during the current trace ?
   *
   * @param object The object to query.
   * @return True if the object will not move.
   */
  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(ConcMark.CONC_MARK, object)) {
      return !MarkRegion.relocationRequired(MarkRegion.of(object.toAddress()));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  @Override
  public void processRoots() {
    // Do nothing
  }
}
