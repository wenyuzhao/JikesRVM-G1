package org.mmtk.plan.concurrent.pureg1;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class PureG1RedirectTraceLocal extends TraceLocal {

  RemSet.Processor processor = new RemSet.Processor(this);

  public void linearUpdatePointers(AddressArray relocationSet, boolean concurrent) {
    processor.updatePointers(relocationSet, concurrent, PureG1.markBlockSpace);
  }

  public PureG1RedirectTraceLocal(Trace trace) {
    super(PureG1.SCAN_REDIRECT, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(PureG1.MC, object)) {
      return PureG1.markBlockSpace.isLive(object);
    }
    return super.isLive(object);
  }

  @Inline
  public void processEdge(ObjectReference source, Address slot) {
    super.processEdge(source, slot);
    ObjectReference ref = slot.loadObjectReference();
    if (!ref.isNull() && Space.isInSpace(PureG1.MC, ref)) {
      Address block = MarkBlock.of(VM.objectModel.objectStartRef(ref));
      if (block.NE(MarkBlock.of(VM.objectModel.objectStartRef(source)))) {
        Address card = MarkBlock.Card.of(VM.objectModel.objectStartRef(source));
        RemSet.addCard(block, card);
      }
    }
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    //return processor.updateObject(object);
    if (object.isNull()) return object;
    if (Space.isInSpace(PureG1.MC, object)) {
      return PureG1.markBlockSpace.traceEvacuateObject(this, object, PureG1.ALLOC_MC);
    }
    return object;
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(PureG1.MC, object)) {
      return !MarkBlock.relocationRequired(MarkBlock.of(VM.objectModel.objectStartRef(object)));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  @Override
  @Inline
  protected void processRememberedSets() {
    processor.processRemSets(PureG1Collector.relocationSet, false, PureG1.markBlockSpace);
  }
}
