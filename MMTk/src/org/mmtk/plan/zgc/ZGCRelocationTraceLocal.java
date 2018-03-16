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
package org.mmtk.plan.zgc;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Space;
import org.mmtk.policy.zgc.Block;
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
public class ZGCRelocationTraceLocal extends TraceLocal {

  public ZGCRelocationTraceLocal(Trace trace) {
    super(ZGC.SCAN_RELOCATE, trace);
  }

  /****************************************************************************
   *
   * Externally visible Object processing and tracing
   */

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(ZGC.Z, object))
      return ZGC.zSpace.isLive(object);
    return super.isLive(object);
  }

  @Override
  public void prepare() {
    super.prepare();
    Log.writeln("Memory: " + VM.activePlan.global().getPagesUsed() + " / " + VM.activePlan.global().getTotalPages());
    Log.writeln("ZPAGE SIZE " + Block.count());
    int aliveSizeInRelocationSet = 0;
    int useableBytesForCopying = (int) (VM.activePlan.global().getPagesAvail() * (1.0 - Block.METADATA_PAGES_PER_REGION / EmbeddedMetaData.PAGES_IN_REGION) * Constants.BYTES_IN_PAGE);

    for (Address zPage : Block.iterate()) {
      Log.write("#Block " + zPage + ": " + Block.usedSize(zPage) + "/" + Block.BYTES_IN_BLOCK);
      int usedSize = Block.usedSize(zPage);
      if (usedSize <= (Block.BYTES_IN_BLOCK >> 1)) {
        if (aliveSizeInRelocationSet + usedSize <= useableBytesForCopying) {
          Log.write(" relocate");
          Block.setRelocationState(zPage, true);
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
    for (Address zPage : Block.iterate()) {
      if (/*Block.relocationRequired(zPage) ||*/ Block.usedSize(zPage) == 0) {
        Log.writeln("#Block " + zPage + ": " + Block.usedSize(zPage) + "/" + Block.BYTES_IN_BLOCK + " released");
        Block.setRelocationState(zPage, false);
        ZGC.zSpace.release(zPage);
      } else {
        Log.writeln("#Block " + zPage + ": " + Block.usedSize(zPage) + "/" + Block.BYTES_IN_BLOCK);
      }
    }
    lock.release();
    Log.writeln("Memory: " + VM.activePlan.global().getPagesUsed() + " / " + VM.activePlan.global().getTotalPages());
    /*
    lock.acquire();
    Address zPage = Block.fromPages.head();
    while (!zPage.isZero()) {
      Address currentZPage = zPage;
      zPage = ZFreeList.next(zPage);

      Log.write("#Block " + currentZPage + ": " + Block.usedSize(currentZPage) + "/" + Block.USEABLE_BYTES);
      if (Block.relocationRequired(currentZPage)) {
        Log.write(" released");
        Log.flush();
        ZGC.zSpace.release(currentZPage);
      }
      Log.writeln();
    };
    lock.release();
*/
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    //Log.writeln("ZGCRelocationTraceLocal.traceObject");
    if (object.isNull()) return object;
    if (Space.isInSpace(ZGC.Z, object))
      return ZGC.zSpace.traceRelocateObject(this, object, ZGC.ALLOC_Z);
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
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ZGC.zSpace.inImmixDefragCollection());
    if (Space.isInSpace(ZGC.Z, object)) {
      return !Block.relocationRequired(Block.of(object.toAddress()));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
