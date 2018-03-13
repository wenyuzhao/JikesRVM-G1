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
import org.mmtk.policy.zgc.ZPage;
import org.mmtk.utility.Log;
import org.mmtk.utility.deque.ObjectReferenceDeque;
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
    int i = 0;
    for (Address zPage = ZPage.head(); !zPage.isZero(); zPage = ZPage.next(zPage)) {
      Log.write("#ZPage " + (i++) + ": " + ZPage.usedSize(zPage) + "/" + ZPage.USEABLE_BYTES);
      if (ZPage.usedSize(zPage) <= (ZPage.USEABLE_BYTES >> 2)) {
        Log.write(" relocate");
        ZPage.setRelocationState(zPage, true);
      }
      Log.writeln();
    };
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    Log.writeln("ZGCRelocationTraceLocal.traceObject");
    if (object.isNull()) return object;
    if (Space.isInSpace(ZGC.Z, object))
      return ZGC.zSpace.traceObjectWithCopy(this, object, ZGC.ALLOC_Z);
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
    return !Space.isInSpace(ZGC.Z, object);
  }
}
