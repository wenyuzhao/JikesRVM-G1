package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Region;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public abstract class G1EvacuationTraceLocal extends TraceLocal {

  public final RemSet.Processor processor = new RemSet.Processor(this, G1.regionSpace);

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

  @Inline
  public void processEdge(ObjectReference source, Address slot) {
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
  public void processRemSets() {
    processor.processRemSets(G1.relocationSet, false, !global().nurseryGC(), G1.regionSpace, PauseTimePredictor.remSetCardScanningTimer);
  }

  @Inline
  private static G1 global() {
    return (G1) VM.activePlan.global();
  }
}
