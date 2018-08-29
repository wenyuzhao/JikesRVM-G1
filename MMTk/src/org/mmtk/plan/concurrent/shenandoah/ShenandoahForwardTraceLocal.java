package org.mmtk.plan.concurrent.shenandoah;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;

@Uninterruptible
public class ShenandoahForwardTraceLocal extends TraceLocal {

  public ShenandoahForwardTraceLocal(Trace trace) {
    super(Shenandoah.SCAN_FORWARD, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(Shenandoah.RS, object)) {
      return Shenandoah.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

  @Override
  @Inline
  public void processEdge(ObjectReference source, Address slot) {
    ObjectReference oldObject, newObject;
    do {
      oldObject = slot.prepareObjectReference();//VM.activePlan.global().loadObjectReference(slot);
      newObject = traceObject(oldObject, false);
//      if (oldObject.toAddress().EQ(newObject.toAddress())) return;
    } while (!slot.attempt(oldObject, newObject));
//    ObjectReference object = VM.activePlan.global().loadObjectReference(slot);
//    ObjectReference newObject = traceObject(object, false);
//    if (overwriteReferenceDuringTrace()) {
//      VM.activePlan.global().storeObjectReference(slot, newObject);
//    }
  }

  @Inline
  public final void processRootEdge(Address slot, boolean untraced) {
    ObjectReference oldObject, newObject;
    do {
      oldObject = slot.prepareObjectReference();//VM.activePlan.global().loadObjectReference(slot);
      newObject = traceObject(oldObject, true);
//      if (oldObject.toAddress().EQ(newObject.toAddress())) return;
    } while (!slot.attempt(oldObject, newObject));
//    ObjectReference object;
//    if (untraced) object = slot.loadObjectReference();
//    else     object = VM.activePlan.global().loadObjectReference(slot);
//    ObjectReference newObject = traceObject(object, true);
//    if (overwriteReferenceDuringTrace()) {
//      if (untraced) slot.store(newObject);
//      else     VM.activePlan.global().storeObjectReference(slot, newObject);
//    }
  }

  public final void processInteriorEdge(ObjectReference target, Address slot, boolean root) {
    VM.assertions.fail("unreachable");
    Address interiorRef = slot.loadAddress();
    Offset offset = interiorRef.diff(target.toAddress());
    ObjectReference newTarget = traceObject(target, root);
    if (VM.VERIFY_ASSERTIONS) {
      if (offset.sLT(Offset.zero()) || offset.sGT(Offset.fromIntSignExtend(1 << 24))) {
        // There is probably no object this large
        Log.writeln("ERROR: Suspiciously large delta to interior pointer");
        Log.writeln("       object base = ", target);
        Log.writeln("       interior reference = ", interiorRef);
        Log.writeln("       delta = ", offset);
        VM.assertions._assert(false);
      }
    }
    if (overwriteReferenceDuringTrace()) {
      slot.store(newTarget.toAddress().plus(offset));
    }
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    ObjectReference newObject;

    if (Space.isInSpace(Shenandoah.RS, object)) {
      newObject = Shenandoah.regionSpace.traceForwardObject(this, object);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!RegionSpace.ForwardingWord.isForwarded(newObject));
    } else {
      newObject = super.traceObject(object);
    }

    return newObject;
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(Shenandoah.RS, object)) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
