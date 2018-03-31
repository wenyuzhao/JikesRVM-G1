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
package org.mmtk.plan.markcopy;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Space;
import org.mmtk.policy.MarkBlock;
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
public class MarkCopyRelocationTraceLocal extends TraceLocal {

  public MarkCopyRelocationTraceLocal(Trace trace) {
    super(MarkCopy.SCAN_RELOCATE, trace);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(MarkCopy.MC, object))
      return MarkCopy.markBlockSpace.isLive(object);
    return super.isLive(object);
  }

  @Override
  public void prepare() {
    super.prepare();
    if (VM.DEBUG) {
      Log.write("Memory: ", VM.activePlan.global().getPagesUsed());
      Log.writeln(" / ", VM.activePlan.global().getTotalPages());
      Log.writeln("BLOCK SIZE ", MarkBlock.count());
    }
    int aliveSizeInRelocationSet = 0;
    int useableBytesForCopying = (int) (VM.activePlan.global().getPagesAvail() * (1.0 - MarkBlock.METADATA_PAGES_PER_REGION / EmbeddedMetaData.PAGES_IN_REGION) * Constants.BYTES_IN_PAGE);

    lock.acquire();
    Address region = MarkBlock.firstBlock();
    int visitedPages = 0;
    while (region != null) {
      visitedPages++;
      if (VM.DEBUG) {
        Log.write("#Block ", region);
        Log.write(": ", MarkBlock.usedSize(region));
        Log.write("/", MarkBlock.BYTES_IN_BLOCK);
      }
      int usedSize = MarkBlock.usedSize(region);
      if (usedSize <= (MarkBlock.BYTES_IN_BLOCK >> 1)) {
        if (aliveSizeInRelocationSet + usedSize <= useableBytesForCopying) {
          if (VM.DEBUG) Log.write(" relocate");
          MarkBlock.setRelocationState(region, true);
          aliveSizeInRelocationSet += usedSize;
        }
      }
      if (VM.DEBUG) Log.writeln();
      region = MarkBlock.nextBlock(region);
    }
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(visitedPages == MarkBlock.count(), "Invalid iteration");
    lock.release();
  }

  static Lock lock = VM.newLock("RelocationGlobal");

  @Override
  public void release() {
    super.release();
    lock.acquire();
    int visitedPages = 0;
    Address region = MarkBlock.firstBlock();
    while (region != null) {
      Address nextRegion = MarkBlock.nextBlock(region);
      if (MarkBlock.relocationRequired(region)) {
        if (VM.DEBUG) {
          Log.write("#Block ", region);
          Log.write(": ", MarkBlock.usedSize(region));
          Log.write("/", MarkBlock.BYTES_IN_BLOCK);
          Log.writeln(" released");
        }
        MarkCopy.markBlockSpace.release(region);
      } else {
        visitedPages++;
        if (VM.DEBUG) {
          Log.write("#Block ", region);
          Log.write(": ", MarkBlock.usedSize(region));
          Log.writeln("/", MarkBlock.BYTES_IN_BLOCK);
        }
      }
      region = nextRegion;
    }
    if (VM.VERIFY_ASSERTIONS) {
      if (visitedPages != MarkBlock.count()) {
        Log.write(visitedPages);
        Log.writeln(" / ", MarkBlock.count());
      }
      VM.assertions._assert(visitedPages == MarkBlock.count(), "Invalid iteration");
    }
    lock.release();
    if (VM.DEBUG) {
      Log.write("Memory: ", VM.activePlan.global().getPagesReserved());
      Log.write(" / ", VM.activePlan.global().getTotalPages());
      Log.writeln(", ", MarkCopy.markBlockSpace.availablePhysicalPages());
    }
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(MarkCopy.MC, object))
      return MarkCopy.markBlockSpace.traceRelocateObject(this, object, MarkCopy.ALLOC_MC);
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
    if (Space.isInSpace(MarkCopy.MC, object)) {
      return !MarkBlock.relocationRequired(MarkBlock.of(VM.objectModel.objectStartRef(object)));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
