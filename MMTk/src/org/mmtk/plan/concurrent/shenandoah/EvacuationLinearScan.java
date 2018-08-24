package org.mmtk.plan.concurrent.shenandoah;


import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.ForwardingTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.policy.RegionSpace.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;


@Uninterruptible
public class EvacuationLinearScan extends LinearScan {

  private final TransitiveClosure updateReferencesTrace = new TransitiveClosure() {
    @Inline @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
      ObjectReference ref = slot.loadObjectReference();
      if (!ref.isNull() && Space.isInSpace(Shenandoah.RS, ref)) {
        ObjectReference forwarded = ForwardingTable.getForwardingPointer(ref);
        if (!forwarded.isNull()) {
          slot.store(forwarded);
        }
      }
    }
  };

  @Inline
  public void forward(ObjectReference object) {
    ObjectReference newObject = ForwardingWord.forwardObject(object, Shenandoah.ALLOC_RS);
    Shenandoah.regionSpace.writeMarkState(newObject);
//    VM.scanning.scanObject(updateReferencesTrace, newObject);
    // Set forwarding pointer in the off-heap table
    ForwardingTable.setForwardingPointer(object, newObject);
    if (VM.VERIFY_ASSERTIONS) {
//      VM.activePlan.mutator().getLog().write(object);
//      VM.activePlan.mutator().getLog().writeln(" -> ", newObject);
      Log.write(object);
      Log.writeln(" -> ", newObject);
    }
  }

  @Inline
  public void scan(ObjectReference object) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(Space.isInSpace(Shenandoah.RS, object));
      VM.assertions._assert(Region.relocationRequired(Region.of(object)));
    }
    if (Shenandoah.regionSpace.isMarked(object)) {
      forward(object);
    }
  }
}
