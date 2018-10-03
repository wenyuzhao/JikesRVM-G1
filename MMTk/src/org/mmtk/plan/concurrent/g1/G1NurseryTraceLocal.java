package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.policy.*;
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
      if (!Region.allocated(Region.of(object))) return false;
      return G1.regionSpace.isLive(object);
    }
    return super.isLive(object);
//    if (Space.isInSpace(G1.G1, object)) {
//      if (Region.relocationRequired(Region.of(object)))
//        return RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(object); //G1.regionSpace.isLive(object);
//    }
//    return true;
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    if (Space.isInSpace(G1.G1, object)) {
      int allocator = Region.kind(Region.of(object)) == Region.EDEN ? G1.ALLOC_SURVIVOR : G1.ALLOC_OLD;
      if (!Region.allocated(Region.of(object))) return object;
      return G1.regionSpace.traceEvacuateObject(this, object, allocator, PauseTimePredictor.evacuationTimer);
    } else {
      return super.traceObject(object);
    }

//    if (G1.regionSpace.contains(object)) {// && Region.relocationRequired(Region.of(object))) {
//    if (Space.isInSpace(G1.G1, object)) {
//      int allocator = Region.kind(Region.of(object)) == Region.EDEN ? G1.ALLOC_SURVIVOR : G1.ALLOC_OLD;
//      return G1.regionSpace.traceEvacuateObject(this, object, allocator, PauseTimePredictor.evacuationTimer);
//    }

//    if (!G1.regionSpace.contains(object)) {
//      return super.traceObject(object);
//    } else {
//      return object;
//    }
  }

  /**
   * {@inheritDoc}
   */
//  @Inline
//  @Override
//  public ObjectReference getForwardedReference(ObjectReference object) {
//    ObjectReference rtn = traceObject(object);
////    if (!rtn.isNull()) processNode(rtn);
//    return rtn;
//  }

  /**
   * {@inheritDoc}
   */
//  @Inline
//  @Override
//  public ObjectReference retainReferent(ObjectReference object) {
//    ObjectReference rtn = traceObject(object);
////    if (!rtn.isNull()) processNode(rtn);
//    return rtn;
//  }

  /**
   * {@inheritDoc}
   */
//  @Inline
//  @Override
//  public ObjectReference retainForFinalize(ObjectReference object) {
//    ObjectReference rtn = traceObject(object);
////    if (!rtn.isNull()) processNode(rtn);
//    return rtn;
//  }

  public final RemSet.Processor processor = new RemSet.Processor(this, G1.regionSpace, true);
  @Inline
  public void processRemSets() {
    processor.processRemSets(G1.relocationSet, false, true, G1.regionSpace, PauseTimePredictor.remSetCardScanningTimer);
  }
}
