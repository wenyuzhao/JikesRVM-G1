package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.policy.LargeObjectSpace;
import org.mmtk.policy.Region;
import org.mmtk.policy.SegregatedFreeListSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BlockAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class G1NurseryTraceLocal extends G1EvacuationTraceLocal {

  public G1NurseryTraceLocal(Trace trace) {
    super(G1.SCAN_NURSERY, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(G1.G1, object)) {
      return G1.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

//  @Inline
//  @Override
//  public ObjectReference traceObject(ObjectReference object, boolean root) {
//    ObjectReference newObject = super.traceObject(object, root);
//
//    if (!object.isNull() && root) {
//      this.processNode(newObject);
//    }
//
//    return newObject;
//  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

//    Space space = Space.getSpaceForObject(object);
//    if (space instanceof SegregatedFreeListSpace) {
//      if (!BlockAllocator.checkBlockMeta(VM.objectModel.objectStartRef(object))) {
//        return object;
//      }
//    } if (space instanceof LargeObjectSpace) {
//      if (!((LargeObjectSpace) space).isInToSpace(VM.objectModel.objectStartRef(object))) {
//        return object;
//      }
//    }

//    if (VM.VERIFY_ASSERTIONS) {
//      if (!VM.debugging.validRef(object)) {
//        Address region = Region.of(object);
//        if (Space.isInSpace(G1.G1, object)) {
//          Log.writeln("generation=", Region.kind(region));
//          Log.writeln(Region.allocated(region) ? "alloc=true" : "alloc=false");
//          Log.writeln(Region.relocationRequired(region) ? "reloc=true" : "reloc=false");
//        } else {
//          Log.write("Space ");
//          Log.writeln(Space.getSpaceForObject(object).getName());
//        }
//        VM.objectModel.dumpObject(object);
//        VM.assertions.fail("");
//      }
//      VM.assertions._assert(VM.debugging.validRef(object));
//    }
//    Region.Card.updateCardMeta(object);

    ObjectReference newObject = object;

    if (G1.regionSpace.contains(object) && Region.relocationRequired(Region.of(object))) {
      int allocator = Region.kind(Region.of(object)) == Region.EDEN ? G1.ALLOC_SURVIVOR : G1.ALLOC_OLD;
      newObject = G1.regionSpace.traceEvacuateObject(this, object, allocator, PauseTimePredictor.evacuationTimer);
    }

    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isLive(newObject));

    return newObject;
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference getForwardedReference(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    processNode(rtn);
    return rtn;
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference retainReferent(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    processNode(rtn);
    return rtn;
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference retainForFinalize(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    processNode(rtn);
    return rtn;
  }
}
