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
  RemSet.Processor processor = new RemSet.Processor(this);

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
    if (root) {
      if (object.isNull()) return object;
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!remSetsProcessing);

      if (VM.VERIFY_ASSERTIONS) {
        if (!VM.debugging.validRef(object)) Log.writeln(isLive(object) ? " live" : " dead");
        VM.assertions._assert(VM.debugging.validRef(object));
      }

      Region.Card.updateCardMeta(object);

      ObjectReference newObject;
      if (Space.isInSpace(PureG1.MC, object)) {
        newObject = PureG1.regionSpace.traceEvacuateObject(this, object, PureG1.ALLOC_MC, true);
      } else {
        newObject = super.traceObject(object);
      }
      Region.Card.updateCardMeta(newObject);
      this.processNode(newObject);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isLive(newObject));
      return newObject;
    } else {
      return traceObject(object);
    }
  }

  @Inline
  public void processEdge(ObjectReference source, Address slot) {
    super.processEdge(source, slot);

    ObjectReference ref = slot.loadObjectReference();
    if (!ref.isNull() && Space.isMappedObject(ref) && Space.isInSpace(PureG1.MC, ref)) {
      Address block = Region.of(VM.objectModel.objectStartRef(ref));
      if (block.NE(Region.of(VM.objectModel.objectStartRef(source)))) {
        Region.Card.updateCardMeta(source);
        Address card = Region.Card.of(source);
        //if (remSetsProcessing)
        RemSet.addCard(block, card);
      }
    }
  }

  @Inline
  @Override
  public void scanObject(ObjectReference object) {
    if (object.isNull()) return;
    if (remSetsProcessing) {
      if (!Space.isMappedObject(object)) return;
    }
    VM.assertions._assert(VM.debugging.validRef(object));
    super.scanObject(object);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    if (remSetsProcessing) {
      if (Space.isInSpace(PureG1.MC, object)) {
        // Mark only, not tracing children
        ObjectReference newObject = PureG1.regionSpace.traceEvacuateObject(this, object, PureG1.ALLOC_MC, true);
        return newObject;
      }
      return object;
    } else {
      if (VM.VERIFY_ASSERTIONS) {
        if (!VM.debugging.validRef(object)) Log.writeln(isLive(object) ? " live" : " dead");
        VM.assertions._assert(VM.debugging.validRef(object));
      }
      Region.Card.updateCardMeta(object);
      if (Space.isInSpace(PureG1.MC, object)) {
        ObjectReference newObject = PureG1.regionSpace.traceEvacuateObject(this, object, PureG1.ALLOC_MC, true);
        Region.Card.updateCardMeta(newObject);
        VM.assertions._assert(isLive(newObject));
        return newObject;
      }
      ObjectReference ref = super.traceObject(object);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isLive(ref));
      return ref;
    }
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(PureG1.MC, object)) {
      return !Region.relocationRequired(Region.of(VM.objectModel.objectStartRef(object)));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  boolean remSetsProcessing = false;

  //@Override
  @Inline
  public void processRemSets() {
    processor.processRemSets(PureG1.relocationSet, false, PureG1.regionSpace);
  }
}
