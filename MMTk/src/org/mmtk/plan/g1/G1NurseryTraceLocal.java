package org.mmtk.plan.g1;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Space;
import org.mmtk.policy.region.Card;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.RegionSpace;
import org.mmtk.policy.region.RemSet;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class G1NurseryTraceLocal extends TraceLocal {
  public int copyBytes = 0;

  public G1NurseryTraceLocal(Trace trace) {
    super(G1.SCAN_NURSERY, trace);
  }

  @Override
  public void release() {
    super.release();
    G1.predictor.stat.nurserySurvivedBytes.add(copyBytes);
  }

  @Override
  @Inline
  public void processEdge(ObjectReference src, Address slot) {
    ObjectReference object = VM.activePlan.global().loadObjectReference(slot);
    ObjectReference newObject = traceObject(object, false);
    if (overwriteReferenceDuringTrace()) {
      if (G1.ENABLE_REMEMBERED_SETS) {
        // src.slot -> new_object
        if (RegionSpace.isCrossRegionRef(src, slot, newObject) && Space.isInSpace(G1.REGION_SPACE, newObject)) {
          Address card = Card.of(src);
//          RemSet.addCard(Region.of(newObject), card);
          if (Space.isInSpace(G1.REGION_SPACE, card) && Region.getInt(Region.of(card), Region.MD_GENERATION) != Region.OLD) {
            // Do nothing
          } else {
            RemSet.addCard(Region.of(newObject), card);
          }
        }
      }
      VM.activePlan.global().storeObjectReference(slot, newObject);
    }
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(G1.ENABLE_GENERATIONAL_GC);
    if (object.isNull()) return false;
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      if (Region.getBool(Region.of(object), Region.MD_RELOCATE)) {
        return ForwardingWord.isForwardedOrBeingForwarded(object);
      } else {
        return true;
      }
    }
    return true;
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(G1.ENABLE_GENERATIONAL_GC);
    if (object.isNull()) return object;
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      Address region = Region.of(object);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.getBool(region, Region.MD_ALLOCATED));
      if (Region.getBool(region, Region.MD_RELOCATE)) {
        if (G1.regionSpace.isLivePrev(object)) {
          int allocator = G1.pickCopyAllocator(object);
          return G1.regionSpace.traceEvacuateObjectInCSet(this, object, allocator);
        } else {
          return ObjectReference.nullReference();
        }
      } else {
        return object;
      }
    } else {
      return object;
    }
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      return false;
    } else {
      return true;
    }
  }
}
