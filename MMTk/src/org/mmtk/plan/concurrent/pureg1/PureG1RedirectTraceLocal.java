package org.mmtk.plan.concurrent.pureg1;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.*;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class PureG1RedirectTraceLocal extends TraceLocal {
  RemSet.Processor processor = new RemSet.Processor(this, PureG1.regionSpace);

  public PureG1RedirectTraceLocal(Trace trace) {
    super(PureG1.SCAN_REDIRECT, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(PureG1.MC, object)) {
      return PureG1.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

  @Inline
  @Override
  public ObjectReference traceObject(ObjectReference object, boolean root) {
    ObjectReference newObject = super.traceObject(object, root);

    if (!object.isNull() && root) {
      this.processNode(newObject);
    }

    return newObject;
  }
//  @Inline
//  public void processEdge(ObjectReference source, Address slot) {
//    super.processEdge(source, slot);
//    Region.Card.updateCardMeta(source);
//  }
  @Inline
  public void processEdge(ObjectReference source, Address slot) {
    //VM.assertions._assert(VM.debugging.validRef(source));
//    boolean referenceIsRelocated = false;
//    ObjectReference oldRef = slot.loadObjectReference();
//    if (!oldRef.isNull() && Space.isInSpace(PureG1.MC, oldRef) && Region.relocationRequired(Region.of(oldRef))) {
//      referenceIsRelocated = true;
//    }

    super.processEdge(source, slot);

    ObjectReference ref = slot.loadObjectReference();
    //if (VM.VERIFY_ASSERTIONS) {
    //  VM.assertions._assert(ref.isNull() || Space.isMappedObject(ref));
    //  }
    if (!ref.isNull() && Space.isMappedObject(ref) && Space.isInSpace(PureG1.MC, ref)) {
//    if (referenceIsRelocated) {
      Address block = Region.of(ref);
      if (block.NE(Region.of(source))) {
        Region.Card.updateCardMeta(source);
        Address card = Region.Card.of(source);
        RemSet.addCard(block, card);
      }
    }
  }
//
//  @Inline
//  @Override
//  public void scanObject(ObjectReference object) {
//    VM.assertions._assert(VM.debugging.validRef(object));
//    super.scanObject(object);
//  }
/*
  @Inline
  public ObjectReference getForwardedReference(ObjectReference object) {
    return traceObject(object, true);
  }

  @Inline
  public ObjectReference retainReferent(ObjectReference object) {
    return traceObject(object, true);
  }

  @Inline
  public ObjectReference retainForFinalize(ObjectReference object) {
    return traceObject(object, true);
  }
*/
  boolean traceFinalizables = false;

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    /*if (remSetsProcessing) {
      if (Space.isInSpace(PureG1.MC, object)) {
        // Mark only, not tracing children
        ObjectReference newObject = PureG1.regionSpace.traceEvacuateObject(this, object, PureG1.ALLOC_MC, true);
        return newObject;
      }
      return object;
    } else {*/
//      if (VM.VERIFY_ASSERTIONS) {
//        if (!VM.debugging.validRef(object)) Log.writeln(isLive(object) ? " live" : " dead");
//        VM.assertions._assert(VM.debugging.validRef(object));
//      }
      Region.Card.updateCardMeta(object);
      ObjectReference newObject = object;



      if (Space.isInSpace(PureG1.MC, object)) {
        if (traceFinalizables && !isLive(object)) {
          Address region = Region.of(object);
          Region.updateBlockAliveSize(region, object);
          newObject = PureG1.regionSpace.traceEvacuateObject(this, object, PureG1.ALLOC_MC, true);
        } else {
          newObject = PureG1.regionSpace.traceEvacuateObject(this, object, PureG1.ALLOC_MC, false);
        }
        Region.Card.updateCardMeta(newObject);
      } else {
        if (traceFinalizables) {
          newObject = super.traceObject(object);
        }
      }

//      if (VM.VERIFY_ASSERTIONS) {
//        VM.assertions._assert(VM.debugging.validRef(newObject));
//        VM.assertions._assert(isLive(newObject));
//      }
//      Region.Card.updateCardMeta(newObject);
      return newObject;
    //}
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(PureG1.MC, object)) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  //boolean remSetsProcessing = false;

  //@Override
  @Inline
  public void processRemSets() {
    processor.processRemSets(PureG1.relocationSet, false, PureG1.regionSpace);
  }
}
