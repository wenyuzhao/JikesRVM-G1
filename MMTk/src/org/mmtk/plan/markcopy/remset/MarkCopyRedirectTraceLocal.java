package org.mmtk.plan.markcopy.remset;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class MarkCopyRedirectTraceLocal extends TraceLocal {

  RemSet.Processor processor = new RemSet.Processor(this);

  public void linearUpdatePointers(AddressArray relocationSet, boolean concurrent) {
    processor.updatePointers(relocationSet, concurrent);
  }

  public MarkCopyRedirectTraceLocal(Trace trace) {
    super(MarkCopy.SCAN_REDIRECT, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(MarkCopy.MC, object))
      return MarkCopy.markBlockSpace.isLive(object);
    return super.isLive(object);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    return processor.updateObject(object);
  }
}
