package org.mmtk.plan.concurrent.shenandoah;


import org.mmtk.policy.RegionSpace.ForwardingWord;
import org.mmtk.utility.alloc.LinearScan;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;


@Uninterruptible
public final class EvacuationLinearScan extends LinearScan {
  @Inline
  @Override
  public final void scan(ObjectReference object) {
    if (Shenandoah.regionSpace.isLive(object)) {
      Word priorStatusWord = ForwardingWord.attemptToForward(object);
      if (!ForwardingWord.stateIsForwardedOrBeingForwarded(priorStatusWord)) {
        ForwardingWord.forwardObject(object, Shenandoah.ALLOC_RS);
      }
    }
  }
}
