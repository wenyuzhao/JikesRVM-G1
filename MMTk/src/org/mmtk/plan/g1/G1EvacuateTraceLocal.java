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
public class G1EvacuateTraceLocal extends TraceLocal {

  public G1EvacuateTraceLocal(Trace trace) {
    super(G1.SCAN_EVACUATE, trace);
  }

  @Override
  @Inline
  public void processEdge(ObjectReference src, Address slot) {
    ObjectReference object = VM.activePlan.global().loadObjectReference(slot);
    ObjectReference newObject = traceObject(object, false);
    if (overwriteReferenceDuringTrace()) {
      if (G1.ENABLE_REMEMBERED_SETS) {
        // src.slot -> new_object
        // Skip if `src` is in nursery
        if (RegionSpace.isCrossRegionRef(src, slot, newObject) && Space.isInSpace(G1.REGION_SPACE, newObject)) {
          Address card = Card.of(src);
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
    if (object.isNull()) return false;

    if (G1.ENABLE_REMEMBERED_SETS) {
      if (Space.isInSpace(G1.REGION_SPACE, object)) {
        Address region = Region.of(object);
        if (Region.getBool(region, Region.MD_RELOCATE)) {
          return ForwardingWord.isForwardedOrBeingForwarded(object);
        } else {
          return true;
//          return G1.regionSpace.isLivePrev(object);
        }
      } else {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Space.isMappedObject(object));
        return true;
//        return super.isLive(object);
      }
    } else {
      if (Space.isInSpace(G1.REGION_SPACE, object)) {
        if (ForwardingWord.isForwardedOrBeingForwarded(object)) return true;
        return G1.regionSpace.isLiveNext(object);
      }
      return super.isLive(object);
    }
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    if (G1.ENABLE_REMEMBERED_SETS) {
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
    } else {
      if (Space.isInSpace(G1.REGION_SPACE, object)) {
        int allocator = G1.pickCopyAllocator(object);
        return G1.regionSpace.traceEvacuateObject(this, object, allocator, null);
      } else {
        return super.traceObject(object);
      }
    }
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
