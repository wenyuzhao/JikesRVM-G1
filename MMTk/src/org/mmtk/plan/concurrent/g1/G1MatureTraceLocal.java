package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.policy.*;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class G1MatureTraceLocal extends G1EvacuationTraceLocal {

  public G1MatureTraceLocal(Trace trace) {
    super(G1.SCAN_MATURE, trace);
  }

  @Inline
  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(G1.G1, object)) return G1.regionSpace.isLive(object);
    return super.isLive(object);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
//    if (object.isNull()) return object;

//    if (VM.VERIFY_ASSERTIONS) {
//      if (!VM.debugging.validRef(object)) {
//        Log.write("Space: ");
//        Log.writeln(Space.getSpaceForObject(object).getName());
//      }
//      VM.assertions._assert(VM.debugging.validRef(object));
//    }



    if (object.isNull()) return object;
//    Region.Card.updateCardMeta(object);
    if (Space.isInSpace(G1.G1, object)) {
      return G1.regionSpace.traceForwardObject(this, object);
    } else {
      return super.traceObject(object);
    }

//    if (G1.regionSpace.contains(object) && Region.relocationRequired(Region.of(object))) {
//      ObjectReference newObject = G1.regionSpace.traceForwardObject(this, object);
//      return newObject;
//    } else {
//      return object;
//    }
    // else {
//      if (VM.VERIFY_ASSERTIONS) {
//        if (!super.isLive(object)) {
//          Log.write("Space: ");
//          Log.writeln(Space.getSpaceForObject(object).getName());
//          VM.objectModel.dumpObject(object);
//        }
//        VM.assertions._assert(super.isLive(object));
//      }
    //  return super.traceObject(object);
    //}
  }

  public final RemSet.Processor processor = new RemSet.Processor(this, G1.regionSpace, false);
  @Inline
  public void processRemSets() {
    processor.processRemSets(G1.relocationSet, false, false, G1.regionSpace, PauseTimePredictor.remSetCardScanningTimer);
  }
}
