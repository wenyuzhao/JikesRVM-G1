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
package org.mmtk.plan.concurrent.pureg1;

import org.mmtk.plan.Plan;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.utility.ForwardingWord;
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
public class PureG1MarkTraceLocal extends TraceLocal {

  public PureG1MarkTraceLocal(Trace trace) {
    super(PureG1.SCAN_MARK, trace);
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
    if (Space.isInSpace(PureG1.MC, object)) {
      return true;
    }
    return super.isLive(object);
  }

  //@Override
  //@Inline
  //public void processEdge(ObjectReference source, Address slot) {
    /*if (VM.VERIFY_ASSERTIONS) {
      //if (!VM.debugging.validRef(source)) {
      //  VM.objectModel.dumpObject(source);
      //}
      VM.assertions._assert(VM.debugging.validRef(source));
    }
    VM.assertions._assert(!Space.isInSpace(Plan.VM_SPACE, source));*/
    //ObjectReference object = slot.loadObjectReference();//VM.activePlan.global().loadObjectReference(slot);
    /*if (!object.isNull()) {
      if (VM.VERIFY_ASSERTIONS) {
        if (!VM.debugging.validRef(object)) {
          VM.objectModel.dumpObject(source);
          VM.objectModel.dumpObject(object);
        }
        VM.assertions._assert(VM.debugging.validRef(object));
      }
    }*/
    //if (!object.isNull() && Space.isInSpace(PureG1.MC, object)) {
      //if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
        /*VM.objectModel.dumpObject(source);
        VM.objectModel.dumpObject(object);
        Log.write(Space.getSpaceForObject(source).getName());
        Log.write(" object ", source);
        Log.write(".", slot);
        Log.write(": ", object);
        Log.write("(");
        Log.write(Space.getSpaceForObject(object).getName());
        Log.writeln(") is forwarded");*/
        //VM.assertions._assert(false);
      //}

      //Address block = Region.of(VM.objectModel.objectStartRef(object));
      /*if (Region.relocationRequired(block)) {
        //VM.objectModel.dumpObject(source);
        Log.write(Space.getSpaceForObject(source).getName());
        Log.write(" object ", VM.objectModel.objectStartRef(source));
        Log.write("  ", source);
        Log.write(".", slot);
        Log.write(": ", object);
        Log.writeln(" is in released block and not forwarded");
        VM.assertions._assert(false);
      }*/
    //}
    //super.processEdge(source, slot);
  //}

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    //if (VM.VERIFY_ASSERTIONS) {
      //if (!VM.debugging.validRef(object)) {
      //  VM.objectModel.dumpObject(object);
      //}
      //VM.assertions._assert(VM.debugging.validRef(object));
    //}
    if (!isLive(object))
      Region.Card.updateCardMeta(object);
    if (Space.isInSpace(PureG1.MC, object)) {
      return PureG1.regionSpace.traceMarkObject(this, object);
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
    if (Space.isInSpace(PureG1.MC, object)) {
      return true;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
