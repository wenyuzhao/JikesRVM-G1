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

import org.mmtk.plan.*;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.MarkBlockSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Pure;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements the core functionality for a transitive
 * closure over the heap graph.
 */
@Uninterruptible
public class Validation extends TraceLocal {

  RemSet.Processor processor = new RemSet.Processor(this);

  public static final int SCAN_VALIDATE = 2;

  public static final short PREPARE = Phase.createSimple("validate-prepare");
  public static final short CLOSURE = Phase.createSimple("validate-closure");
  public static final short RELEASE = Phase.createSimple("validate-release");


  public static final Trace trace = new Trace(Plan.metaDataSpace);


  public static final short validationPhase = Phase.createComplex("validation", null,
      Phase.scheduleGlobal   (PREPARE),
      Phase.scheduleCollector(PREPARE),
      Phase.scheduleMutator  (PREPARE),
      Phase.scheduleMutator  (Simple.PREPARE_STACKS),
      Phase.scheduleGlobal   (Simple.PREPARE_STACKS),
      Phase.scheduleCollector(Simple.STACK_ROOTS),
      Phase.scheduleGlobal   (Simple.STACK_ROOTS),
      Phase.scheduleCollector(Simple.ROOTS),
      Phase.scheduleGlobal   (Simple.ROOTS),
      Phase.scheduleGlobal   (CLOSURE),
      Phase.scheduleCollector(CLOSURE),
      Phase.scheduleCollector(Simple.SOFT_REFS),
      Phase.scheduleGlobal   (CLOSURE),
      Phase.scheduleCollector(CLOSURE),
      Phase.scheduleCollector(Simple.WEAK_REFS),
      Phase.scheduleCollector(Simple.FINALIZABLE),
      Phase.scheduleGlobal   (CLOSURE),
      Phase.scheduleCollector(CLOSURE),
      Phase.scheduleCollector(Simple.PHANTOM_REFS),
      Phase.scheduleCollector(Simple.FORWARD_REFS),
      Phase.scheduleCollector(Simple.FORWARD_FINALIZABLE),
      Phase.scheduleMutator  (RELEASE),
      Phase.scheduleCollector(RELEASE),
      Phase.scheduleGlobal   (RELEASE)
  );

  public static boolean isValidationPhase(short phaseId) {
    return phaseId == PREPARE || phaseId == CLOSURE || phaseId == RELEASE;
  }

  public Validation() {
    super(SCAN_VALIDATE, trace);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(PureG1.MC, object))
      return PureG1.markBlockSpace.isLive(object);
    return super.isLive(object);
  }

  @Override
  @Inline
  public void processEdge(ObjectReference source, Address slot) {
    VM.assertions._assert(!Space.isInSpace(Plan.VM_SPACE, source));
    ObjectReference object = slot.loadObjectReference();//VM.activePlan.global().loadObjectReference(slot);
    if (!object.isNull() && Space.isInSpace(PureG1.MC, object)) {
      if (ForwardingWord.isForwardedOrBeingForwarded(object)) {
        VM.objectModel.dumpObject(source);
        Log.write(Space.getSpaceForObject(source).getName());
        Log.write(" object ", source);
        Log.write(".", slot);
        Log.write(": ", object);
        Log.writeln(" is forwarded");
        VM.assertions._assert(false);
      }

      Address block = MarkBlock.of(VM.objectModel.objectStartRef(object));
      if (MarkBlock.relocationRequired(block)) {
        VM.objectModel.dumpObject(source);
        Log.write(Space.getSpaceForObject(source).getName());
        Log.write(" object ", VM.objectModel.objectStartRef(source));
        Log.write("  ", source);
        Log.write(".", slot);
        Log.write(": ", object);
        Log.writeln(" is in released block and not forwarded");
        VM.assertions._assert(false);
      }
    }
    super.processEdge(source, slot);
  }


  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    VM.debugging.validRef(object);
    if (Space.isInSpace(PureG1.MC, object)) {
      /*if (!isLive(object) && !MarkBlockSpace.Header.isPreviouslyMarked(object)) {
        VM.objectModel.dumpObject(object);
        Log.write("Object ", object);
        Log.writeln(" was not marked in previous cycle");
        VM.assertions._assert(false);
      }*/
      Address block = MarkBlock.of(VM.objectModel.objectStartRef(object));
      if (MarkBlock.relocationRequired(block)) {
        VM.objectModel.dumpObject(object);
        Log.write("Object ", object);
        Log.writeln(" is in released block");
        VM.assertions._assert(false);
      }
      return PureG1.markBlockSpace.traceMarkObject(this, object);
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
      return !MarkBlock.relocationRequired(MarkBlock.of(VM.objectModel.objectStartRef(object)));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  @Override
  @Inline
  protected void processRememberedSets() {
    //if (!remSetsProcessed) {
    processor.processRemSets(PureG1.relocationSet, false, PureG1.markBlockSpace);
    //remSetsProcessed = true;
    //}
  }
}
