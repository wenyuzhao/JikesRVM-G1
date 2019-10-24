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

import org.mmtk.plan.Plan;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.utility.heap.layout.HeapLayout;
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
  private final ObjectReferenceDeque modbuf;

  public G1MarkTraceLocal(Trace trace, ObjectReferenceDeque modbuf) {
    super(G1.SCAN_MARK, trace);
    this.modbuf = modbuf;
  }

  @Override
  public void prepare() {
    super.prepare();
    modbuf.reset();
  }

  @Override
  public void release() {
    super.release();
    modbuf.reset();
  }

  @Override
  @Inline
  protected boolean overwriteReferenceDuringTrace() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      return G1.regionSpace.isLiveNext(object);
    }
    if (Space.isInSpace(G1.LOS, object)) return Plan.loSpace.isLive(object);
    return true;
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    final int descriptor = HeapLayout.vmMap.getDescriptorForAddress(VM.objectModel.refToAddress(object));
    if (descriptor == G1.REGION_SPACE) return G1.regionSpace.traceMarkObject(this, object);
    if (descriptor == G1.IMMORTAL)     return G1.immortalSpace.traceObject(this, object);
    if (descriptor == G1.LOS)          return G1.loSpace.traceObject(this, object);
    if (descriptor == G1.VM_SPACE)     return object;
    if (VM.VERIFY_ASSERTIONS) {
      Log.writeln("Failing object => ", object);
      Space.printVMMap();
      VM.assertions._assert(false, "No special case for space in traceObject");
    }
    return ObjectReference.nullReference();
  }

  /**
   * Will this object move from this point on, during the current trace ?
   *
   * @param object The object to query.
   * @return True if the object will not move.
   */
  @Override
  @Inline
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      return true;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  @Override
  @Inline
  protected void processRememberedSets() {
    if (G1.ENABLE_CONCURRENT_MARKING || G1.FORCE_DRAIN_MODBUF) {
      ObjectReference obj;
      while (!(obj = modbuf.pop()).isNull()) {
        if (G1.attemptUnlog(obj))
          traceObject(obj);
      }
    }
  }
}
