package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public abstract class G1EvacuationTraceLocal extends TraceLocal {

  public G1EvacuationTraceLocal(int specializedScan, Trace trace) {
    super(specializedScan, trace);
  }

  @Inline
  @Override
  public final ObjectReference traceObject(ObjectReference object, boolean root) {
    ObjectReference newObject = super.traceObject(object, root);

    if (!newObject.isNull() && root) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(newObject));
      this.processNode(newObject);
    }

    return newObject;
  }

  @Inline
  public void processEdge(ObjectReference src, Address slot) {
    super.processEdge(src, slot);
    RemSet.updateEdge(G1.G1, src, slot.loadObjectReference());
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (G1.regionSpace.contains(object) && Region.relocationRequired(Region.of(object))) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  @Inline
  abstract public void processRemSets();
}
