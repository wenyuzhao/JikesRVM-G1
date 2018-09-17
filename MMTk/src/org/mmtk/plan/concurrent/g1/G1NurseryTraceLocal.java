package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.policy.LargeObjectSpace;
import org.mmtk.policy.Region;
import org.mmtk.policy.SegregatedFreeListSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BlockAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class G1NurseryTraceLocal extends G1EvacuationTraceLocal {

  public G1NurseryTraceLocal(Trace trace) {
    super(G1.SCAN_NURSERY, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(G1.G1, object)) {
      return G1.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

//  @Inline
//  @Override
//  public ObjectReference traceObject(ObjectReference object, boolean root) {
//    ObjectReference newObject = super.traceObject(object, root);
//
//    if (!object.isNull() && root) {
//      this.processNode(newObject);
//    }
//
//    return newObject;
//  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
//    if (object.isNull()) return object;

//    ObjectReference newObject = object;

    if (G1.regionSpace.contains(object) && Region.relocationRequired(Region.of(object))) {
      int allocator = Region.kind(Region.of(object)) == Region.EDEN ? G1.ALLOC_SURVIVOR : G1.ALLOC_OLD;
      return G1.regionSpace.traceEvacuateObject(this, object, allocator, PauseTimePredictor.evacuationTimer);
    }

    return object;
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference getForwardedReference(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    processNode(rtn);
    return rtn;
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference retainReferent(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    processNode(rtn);
    return rtn;
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference retainForFinalize(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    processNode(rtn);
    return rtn;
  }
}
