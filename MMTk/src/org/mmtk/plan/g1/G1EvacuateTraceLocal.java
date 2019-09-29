package org.mmtk.plan.g1;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Space;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class G1EvacuateTraceLocal extends TraceLocal {

  public G1EvacuateTraceLocal(Trace trace) {
    super(G1.SCAN_EVACUATE, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      return G1.regionSpace.isLiveNext(object);
    }
    return super.isLive(object);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    ObjectReference newObject;

    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      newObject = G1.regionSpace.traceEvacuateObject(this, object, G1.ALLOC_G1, null);
    } else {
      newObject = super.traceObject(object);
    }

    return newObject;
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
