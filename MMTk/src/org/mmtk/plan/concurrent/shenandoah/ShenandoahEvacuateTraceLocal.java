package org.mmtk.plan.concurrent.shenandoah;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.Space;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class ShenandoahEvacuateTraceLocal extends TraceLocal {

  public ShenandoahEvacuateTraceLocal(Trace trace) {
    super(trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(Shenandoah.RS, object)) {
      return Shenandoah.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    ObjectReference newObject = object;

    if (Space.isInSpace(Shenandoah.RS, object)) {
      if (Region.relocationRequired(Region.of(object))) {
        Word priorStatusWord = RegionSpace.ForwardingWord.attemptToForward(object);
        if (RegionSpace.ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
          newObject = RegionSpace.ForwardingWord.spinAndGetForwardedObject(object, priorStatusWord);
        } else {
          newObject = RegionSpace.ForwardingWord.forwardObject(object, Shenandoah.ALLOC_RS);
        }
      }
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(newObject));
        VM.assertions._assert(VM.debugging.validRef(newObject));
      }
    }

    return newObject;
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(Shenandoah.RS, object)) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
