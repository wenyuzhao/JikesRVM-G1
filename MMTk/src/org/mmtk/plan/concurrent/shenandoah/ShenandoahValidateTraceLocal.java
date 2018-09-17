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

@Uninterruptible
public class ShenandoahValidateTraceLocal extends TraceLocal {

  public ShenandoahValidateTraceLocal(Trace trace) {
    super(trace);
  }

  protected boolean overwriteReferenceDuringTrace() {
    return false;
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
    ObjectReference ref = slot.loadObjectReference();
    if (Shenandoah.regionSpace.contains(ref) && RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(ref)) {
      Log log = VM.activePlan.mutator().getLog();
      log.write("Non-updated reference ");
      log.write(Space.getSpaceForObject(source).getName());
      log.write(" ", source);
      log.write(".", slot);
      log.write(" = ", ref);
      log.writeln(" -> ", RegionSpace.ForwardingWord.getForwardedObject(ref));
      VM.assertions.fail("Non-updated reference");
    }
    super.processEdge(source, slot);
  }

  @Inline
  public final void processRootEdge(Address slot, boolean untraced) {
    ObjectReference ref = slot.loadObjectReference();
    if (Shenandoah.regionSpace.contains(ref) && RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(ref)) {
      Log log = VM.activePlan.mutator().getLog();
      log.write("Non-updated root edge ", ref);
      log.write("@", slot);
      log.writeln(" -> ", RegionSpace.ForwardingWord.getForwardedObject(ref));
      VM.assertions.fail("Non-updated reference");
    }
    super.processRootEdge(slot, untraced);
  }

  public final void processInteriorEdge(ObjectReference target, Address slot, boolean root) {
    VM.assertions.fail("Unreachable");
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference ref, boolean root) {
    if (Shenandoah.regionSpace.contains(ref) && RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(ref)) {
      Log log = VM.activePlan.mutator().getLog();
      if (root) {
        log.write("Non-updated root reference ", ref);
      } else {
        log.write("Non-updated reference ", ref);
      }
      log.writeln(" -> ", RegionSpace.ForwardingWord.getForwardedObject(ref));
      VM.assertions.fail("Non-updated reference");
    }
    return traceObject(ref);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(Shenandoah.RS, object)) {
      return Shenandoah.regionSpace.traceMarkObject(this, object);
    }
    return super.traceObject(object);
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
