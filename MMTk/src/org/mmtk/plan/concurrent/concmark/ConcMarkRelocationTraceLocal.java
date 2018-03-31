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

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
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
    if (Space.isInSpace(ConcMark.RC, object))
      return ConcMark.markRegionSpace.isLive(object);
    return super.isLive(object);
  }

  @Override
  public void prepare() {
    super.prepare();
    Log.write("Memory: ", VM.activePlan.global().getPagesUsed());
    Log.writeln(" / ", VM.activePlan.global().getTotalPages());
    Log.writeln("BLOCK SIZE ", MarkBlock.count());
    int aliveSizeInRelocationSet = 0;
    int useableBytesForCopying = (int) (VM.activePlan.global().getPagesAvail() * (1.0 - MarkBlock.METADATA_PAGES_PER_REGION / EmbeddedMetaData.PAGES_IN_REGION) * Constants.BYTES_IN_PAGE);

    /*
    MarkRegion.resetIterator();
    while (MarkRegion.hasNext()) {
      Address region = MarkRegion.next();
      Log.write("#Block ", region);
      Log.write(": ", MarkRegion.usedSize(region));
      Log.write("/", MarkRegion.BYTES_IN_BLOCK);
      int usedSize = MarkRegion.usedSize(region);
      if (usedSize <= (MarkRegion.BYTES_IN_BLOCK >> 1)) {
        if (aliveSizeInRelocationSet + usedSize <= useableBytesForCopying) {
          Log.write(" relocate");
          MarkRegion.setRelocationState(region, true);
          aliveSizeInRelocationSet += usedSize;
        }
      }
      Log.writeln();
    };
    */
  }

  static Lock lock = VM.newLock("RelocationGlobal");

  @Override
  public void release() {
    super.release();
    lock.acquire();
    int visitedPages = 0;
    /*
    MarkRegion.resetIterator();
    while (MarkRegion.hasNext()) {
      Address region = MarkRegion.next();
      if (MarkRegion.relocationRequired(region)) {
        Log.write("#Block ", region);
        Log.write(": ", MarkRegion.usedSize(region));
        Log.write("/", MarkRegion.BYTES_IN_BLOCK);
        Log.writeln(" released");
        MarkRegion.setRelocationState(region, false);
        ConcMark.markBlockSpace.release(region);
      } else {
        visitedPages++;
        Log.write("#Block ", region);
        Log.write(": ", MarkRegion.usedSize(region));
        Log.writeln("/", MarkRegion.BYTES_IN_BLOCK);
      }
    }
    */
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(visitedPages == MarkBlock.count(), "Invalid iteration");
    lock.release();
    Log.write("Memory: ", VM.activePlan.global().getPagesReserved());
    Log.write(" / ", VM.activePlan.global().getTotalPages());
    Log.writeln(", ", ConcMark.markRegionSpace.availablePhysicalPages());
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(ConcMark.RC, object))
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
    if (Space.isInSpace(ConcMark.RC, object)) {
      return !MarkBlock.relocationRequired(MarkBlock.of(object.toAddress()));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
