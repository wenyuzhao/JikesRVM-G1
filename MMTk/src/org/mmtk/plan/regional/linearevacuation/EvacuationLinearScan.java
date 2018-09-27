package org.mmtk.plan.regional.linearevacuation;


import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace.ForwardingWord;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;


@Uninterruptible
public class EvacuationLinearScan extends LinearScan {
  private static final Atomic.Int atomicCounter = new Atomic.Int();

  @Inline
  public void evacuateRegions() {
    atomicCounter.set(0);
    VM.activePlan.collector().rendezvous();
    int index;
    while ((index = atomicCounter.add(1)) < Regional.relocationSet.length()) {
      Address region = Regional.relocationSet.get(index);
      if (region.isZero() || Region.usedSize(region) == 0) continue;
      Region.linearScan(this, region);
    }
    VM.activePlan.collector().rendezvous();
  }

  @Inline
  public void scan(ObjectReference object) {
    if (Regional.regionSpace.isLive(object)) {
      ForwardingWord.forwardObject(object, Regional.ALLOC_RS);
    }
  }
}
