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
package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Region;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
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
public class G1MarkTraceLocal extends TraceLocal {

  public G1MarkTraceLocal(Trace trace) {
    super(G1.SCAN_MARK, trace);
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
    if (Space.isInSpace(G1.G1, object)) {
      return G1.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(object));

//    if (VM.VERIFY_ASSERTIONS) {
//      if (!VM.debugging.validRef(object)) {
//        Log _log = VM.activePlan.mutator().getLog();
//        _log.write(Space.getSpaceForObject(object).getName());
//        _log.write(" ");
//        _log.flush();
//        VM.objectModel.dumpObject(object);
//      }
//      VM.assertions._assert(VM.debugging.validRef(object));
//    }

//    if (!isLive(object)) Region.Card.assertCardMeta(object);

    if (Space.isInSpace(G1.G1, object)) {
      return G1.regionSpace.traceMarkObject(this, object);
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
    if (Space.isInSpace(G1.G1, object)) {
      return true;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
