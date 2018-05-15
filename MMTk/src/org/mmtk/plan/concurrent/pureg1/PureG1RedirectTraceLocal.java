package org.mmtk.plan.concurrent.pureg1;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
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
  @Inline
  public ObjectReference retainForFinalize(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(PureG1.MC, object))
      return PureG1.markBlockSpace.traceRelocateObject(this, object, PureG1.ALLOC_MC);
    return super.traceObject(object);
  }

  @Override
  @Inline
  public ObjectReference getForwardedFinalizable(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(PureG1.MC, object))
      return PureG1.markBlockSpace.traceRelocateObject(this, object, PureG1.ALLOC_MC);
    return super.traceObject(object);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(PureG1.MC, object))
      return PureG1.markBlockSpace.isLive(object);
    return super.isLive(object);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    return processor.updateObject(object);
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(PureG1.MC, object)) {
      return !MarkBlock.relocationRequired(MarkBlock.of(VM.objectModel.objectStartRef(object)));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
