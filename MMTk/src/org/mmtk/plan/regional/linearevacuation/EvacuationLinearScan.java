package org.mmtk.plan.regional.linearevacuation;


import org.mmtk.policy.RegionSpace.ForwardingWord;
import org.mmtk.utility.alloc.LinearScan;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;


@Uninterruptible
public class EvacuationLinearScan extends LinearScan {
  @Inline
  public void scan(ObjectReference object) {
    if (Regional.regionSpace.isLive(object)) {
      ForwardingWord.forwardObject(object, Regional.ALLOC_RS);
    }
  }
}
