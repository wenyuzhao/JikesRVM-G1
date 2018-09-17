package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.policy.*;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class G1MatureTraceLocal extends G1EvacuationTraceLocal {

  public G1MatureTraceLocal(Trace trace) {
    super(G1.SCAN_MATURE, trace);
  }

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

//    Region.Card.updateCardMeta(object);

    if (Space.isInSpace(G1.G1, object)) {
      return G1.regionSpace.traceForwardObject(this, object);
    } else {
      return super.traceObject(object);
    }
  }
}
