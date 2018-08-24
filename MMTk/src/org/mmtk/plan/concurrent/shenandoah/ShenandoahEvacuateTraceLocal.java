package org.mmtk.plan.concurrent.shenandoah;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.ForwardingTable;
import org.mmtk.policy.Space;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class ShenandoahEvacuateTraceLocal extends TraceLocal {

  public ShenandoahEvacuateTraceLocal(Trace trace) {
    super(Shenandoah.SCAN_EVACUATE, trace);
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

    ObjectReference newObject;

    if (Space.isInSpace(Shenandoah.RS, object)) {
      ObjectReference forwardedObject = ForwardingTable.getForwardingPointer(object);
      newObject = forwardedObject.isNull() ? object : forwardedObject;
    } else {
      newObject = super.traceObject(object);
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
