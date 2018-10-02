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

@Uninterruptible
public abstract class G1EvacuationTraceLocal extends TraceLocal {

  public G1EvacuationTraceLocal(int specializedScan, Trace trace) {
    super(specializedScan, trace);
  }

  @Inline
  @Override
  public ObjectReference traceObject(ObjectReference object, boolean root) {
    ObjectReference newObject = super.traceObject(object, root);

    if (!object.isNull() && root) {
      this.processNode(newObject);
    }

    return newObject;
  }
  private static final Offset LIVE_STATE_OFFSET = VM.objectModel.GC_HEADER_OFFSET().plus(Constants.BYTES_IN_ADDRESS);
  @Inline
  public void processEdge(ObjectReference source, Address slot) {
//    ObjectReference oldRef = slot.loadObjectReference();
//    if (!oldRef.isNull() && VM.VERIFY_ASSERTIONS) {
//      if (!VM.debugging.validRef(oldRef)) {
//        Log log = VM.activePlan.mutator().getLog();
//        log.write("Invalid reference ");
//        log.write(Space.getSpaceForObject(source).getName());
//        log.write(Space.getSpaceForObject(oldRef).getName());
//        log.write(" ", source);
//        log.write(".", slot);
//        log.writeln(" -> ", oldRef);
//        log.writeln("Dead mark (source) = ", source.toAddress().loadWord(LIVE_STATE_OFFSET));
//        log.writeln("Dead mark (ref) = ", oldRef.toAddress().loadWord(LIVE_STATE_OFFSET));
//        if (RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(oldRef)) {
//          log.writeln("ref isForwardedOrBeingForwarded=true");
//        } else {
//          log.writeln("ref isForwardedOrBeingForwarded=false");
//        }
//        if (RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(source)) {
//          log.writeln("source isForwardedOrBeingForwarded=true");
//        } else {
//          log.writeln("source isForwardedOrBeingForwarded=false");
//        }
//        VM.objectModel.dumpObject(oldRef);
//        VM.assertions.fail("Non-updated reference");
//      }
//    }

    super.processEdge(source, slot);

    ObjectReference ref = slot.loadObjectReference();

    if (!ref.isNull() && Space.isInSpace(G1.G1, ref)) {
      Address block = Region.of(ref);
      if (block.NE(Region.of(source))) {
        Address card = Region.Card.of(source);
        Region.Card.updateCardMeta(source);
        RemSet.addCard(block, card);
      }
    }
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

  @Inline
  private static G1 global() {
    return (G1) VM.activePlan.global();
  }
}
