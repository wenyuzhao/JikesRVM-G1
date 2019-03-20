package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace.ForwardingWord;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.utility.Atomic;
//import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;


@Uninterruptible
public class EvacuationLinearScan extends LinearScan {
  private static final Atomic.Int atomicCounter = new Atomic.Int();

  private static final TransitiveClosure updateRemSetTransitiveClosure = new TransitiveClosure() {
    @Override @Inline @Uninterruptible
    public void processEdge(ObjectReference source, Address slot) {
      ObjectReference ref = slot.loadObjectReference();

      if (!ref.isNull() && Space.isInSpace(G1.G1, ref)) {
        Address block = Region.of(ref);
        if (block.NE(Region.of(source))) {
          Address card = Region.Card.of(source);
//          Region.Card.assertCardMeta(source);
          RemSet.addCard(block, card);
        }
      }
    }
  };

  @Inline
  public void evacuateRegions() {
    atomicCounter.set(0);
    VM.activePlan.collector().rendezvous();
    int index;
    totalEvacuationTime = 0;
    totalObjectSize = 0;
    while ((index = atomicCounter.add(1)) < G1.relocationSet.length()) {
      Address region = G1.relocationSet.get(index);
      if (region.isZero() || Region.usedSize(region) == 0) continue;
      Region.linearScan(this, region);
    }
    PauseTimePredictor.updateObjectEvacuationTime(totalObjectSize, totalEvacuationTime);
    VM.activePlan.collector().rendezvous();
  }

  long totalObjectSize = 0;
  long totalEvacuationTime = 0;

  @Inline
  public void scan(ObjectReference object) {
    if (G1.regionSpace.isLive(object)) {
//      if (Region.verbose()) Log.writeln("Evacuate ", object);
      int allocator = Region.kind(Region.of(object)) == Region.EDEN ? G1.ALLOC_SURVIVOR : G1.ALLOC_OLD;
      // Forward
      long time = VM.statistics.nanoTime();
      ObjectReference newObject = ForwardingWord.forwardObject(object, allocator);
      totalEvacuationTime += (VM.statistics.nanoTime() - time);
      totalObjectSize += VM.objectModel.getSizeWhenCopied(newObject);
//      PauseTimePredictor.updateObjectEvacuationTime(VM.objectModel.getSizeWhenCopied(newObject), VM.statistics.nanoTime() - time);

//      VM.scanning.scanObject(updateRemSetTransitiveClosure, newObject);
//      Region.Card.updateCardMeta(newObject);
    } else {
//      object.toAddress().store(VM.objectModel.getObjectEndAddress(object), Region.Card.OBJECT_END_ADDRESS_OFFSET);
    }
  }
}
