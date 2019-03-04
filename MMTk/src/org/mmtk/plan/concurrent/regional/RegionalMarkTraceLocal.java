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
package org.mmtk.plan.concurrent.regional;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.HeaderByte;
import org.mmtk.utility.Log;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements the core functionality for a transitive
 * closure over the heap graph.
 */
@Uninterruptible
public class RegionalMarkTraceLocal extends TraceLocal {

  private final ObjectReferenceDeque modbuf;

  public RegionalMarkTraceLocal(Trace trace, ObjectReferenceDeque modbuf) {
    super(Regional.SCAN_MARK, trace);
    this.modbuf = modbuf;
  }


  @Override
  protected boolean overwriteReferenceDuringTrace() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(Regional.RS, object)) {
      return Regional.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(Regional.RS, object)) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(VM.debugging.validRef(object));
      }
      return Regional.regionSpace.traceMarkObject(this, object);
    }
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
    if (Space.isInSpace(Regional.RS, object)) {
      return true;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  @Override
  protected void processRememberedSets() {
    ObjectReference obj;
    while (!(obj = modbuf.pop()).isNull()) {
//      HeaderByte.markAsUnlogged(obj);
//      if (VM.VERIFY_ASSERTIONS) VM.debugging.validRef(obj);
      traceObject(obj);
    }
    if (Region.verbose()) Log.writeln("SATB Queue processed");
  }
}
