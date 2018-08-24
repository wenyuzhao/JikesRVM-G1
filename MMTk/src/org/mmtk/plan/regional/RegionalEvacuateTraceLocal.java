package org.mmtk.plan.regional;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Space;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class RegionalEvacuateTraceLocal extends TraceLocal {

  public RegionalEvacuateTraceLocal(Trace trace) {
    super(Regional.SCAN_EVACUATE, trace);
  }

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

    ObjectReference newObject;

    if (Space.isInSpace(Regional.RS, object)) {
      newObject = Regional.regionSpace.traceEvacuateObject(this, object, Regional.ALLOC_MC, null);
    } else {
      newObject = super.traceObject(object);
    }

    return newObject;
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(Regional.RS, object)) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
