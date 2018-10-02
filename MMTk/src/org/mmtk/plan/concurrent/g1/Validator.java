package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.*;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class Validator extends TraceLocal {

  public static final short VALIDATE_PREPARE = Phase.createSimple("validate-prepare");
  public static final short VALIDATE_CLOSURE = Phase.createSimple("validate-closure");
  public static final short VALIDATE_RELEASE = Phase.createSimple("validate-release");


  public static short validationPhase = Phase.createComplex("validation-phase", null,
      Phase.scheduleGlobal   (VALIDATE_PREPARE),
      Phase.scheduleCollector(VALIDATE_PREPARE),
      Phase.scheduleMutator  (VALIDATE_PREPARE),
      Phase.scheduleMutator  (Simple.PREPARE_STACKS),
      Phase.scheduleGlobal   (Simple.PREPARE_STACKS),
      Phase.scheduleCollector(Simple.STACK_ROOTS),
      Phase.scheduleGlobal   (Simple.STACK_ROOTS),
      Phase.scheduleCollector(Simple.ROOTS),
      Phase.scheduleGlobal   (Simple.ROOTS),
      Phase.scheduleCollector(VALIDATE_CLOSURE),
      Phase.scheduleCollector(Simple.SOFT_REFS),
      Phase.scheduleCollector(VALIDATE_CLOSURE),
      Phase.scheduleCollector(Simple.WEAK_REFS),
      Phase.scheduleCollector(Simple.FINALIZABLE),
      Phase.scheduleCollector(VALIDATE_CLOSURE),
      Phase.scheduleCollector(Simple.PHANTOM_REFS),
      Phase.scheduleCollector(VALIDATE_CLOSURE),
      Phase.scheduleMutator  (VALIDATE_RELEASE),
      Phase.scheduleCollector(VALIDATE_RELEASE),
      Phase.scheduleGlobal   (VALIDATE_RELEASE)
  );

  public Validator(Trace trace) {
    super(trace);
  }

  protected boolean overwriteReferenceDuringTrace() {
    return false;
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(G1.G1, object)) {
      return G1.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

  @Override
  @Inline
  public void processEdge(ObjectReference source, Address slot) {
    ObjectReference ref = slot.loadObjectReference();
    if (ref.isNull()) return;
    if (!Space.isMappedObject(ref) || (Space.isInSpace(G1.G1, ref) && !Region.allocated(Region.of(ref)))) {
      Log log = VM.activePlan.mutator().getLog();
      log.write("Non-updated reference ");
      log.write(Space.getSpaceForObject(source).getName());
      log.write(isLive(source) ? " live " : " dead ");
      log.write(" ", source);
      log.write(".", slot);
      log.writeln(" -> ", ref);
      VM.objectModel.dumpObject(source);
      VM.objectModel.dumpObject(ref);
      VM.assertions.fail("Non-updated reference");
    }
    if (G1.regionSpace.contains(ref) && RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(ref)) {
      Log log = VM.activePlan.mutator().getLog();
      log.write("Non-updated reference ");
      log.write(Space.getSpaceForObject(source).getName());
      log.write(" ", source);
      log.write(".", slot);
      log.write(" = ", ref);
      log.writeln(" -> ", RegionSpace.ForwardingWord.getForwardedObject(ref));
      VM.assertions.fail("Non-updated reference");
    }
    if (G1.regionSpace.contains(source)) {
      Address srcRegion = Region.of(source);
      if (Region.relocationRequired(srcRegion)) {
        Log log = VM.activePlan.mutator().getLog();
        log.write(Space.getSpaceForObject(source).getName());
        log.write(" object ", source);
        log.writeln(" is relocated");
        VM.assertions.fail("");
      }
    }
    if (G1.regionSpace.contains(ref)) {
      Address srcRegion = Region.of(source);
      Address srcCard = Region.Card.of(source);
      Address refRegion = Region.of(ref);
      if (Region.relocationRequired(refRegion)) {
        Log log = VM.activePlan.mutator().getLog();
        log.write(Space.getSpaceForObject(ref).getName());
        log.write(" object ", ref);
        log.writeln(" is relocated");
        VM.assertions.fail("");
      }
      if (srcRegion.NE(refRegion)) {
//        if (!RemSet.contains(refRegion, srcCard)) {
//          Log log = VM.activePlan.mutator().getLog();
//          log.write(Space.getSpaceForObject(source).getName());
//          log.write("EDGE ", source);
//          log.write(".", slot);
//          log.write(" -> ", ref);
//          log.write("Invalid remembered set: Region ", refRegion);
//          log.writeln(" does not remember ", srcCard);
//          VM.assertions.fail("Invalid remembered set");
//        }
      }
    }
    super.processEdge(source, slot);
  }

  @Inline
  public final void processRootEdge(Address slot, boolean untraced) {
    ObjectReference ref = slot.loadObjectReference();
    if (ref.isNull()) return;
    if (!Space.isMappedObject(ref) || (Space.isInSpace(G1.G1, ref) && !Region.allocated(Region.of(ref)))) {
      Log log = VM.activePlan.mutator().getLog();
      log.write("Non-updated root edge ", ref);
      log.write("@", slot);
      log.writeln(" -> ", RegionSpace.ForwardingWord.getForwardedObject(ref));
      VM.assertions.fail("Non-updated reference");
    }
    if (G1.regionSpace.contains(ref) && RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(ref)) {
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
    if (ref.isNull()) return ref;
    if (G1.regionSpace.contains(ref)) {
      Address srcRegion = Region.of(ref);
      if (Region.relocationRequired(srcRegion) || !Region.allocated(srcRegion)) {
        Log log = VM.activePlan.mutator().getLog();
        log.write(Space.getSpaceForObject(ref).getName());
        log.write(" object ", ref);
        log.writeln(" is relocated");
        VM.assertions.fail("");
      }
    }
    if (G1.regionSpace.contains(ref) && RegionSpace.ForwardingWord.isForwardedOrBeingForwarded(ref)) {
      Log log = VM.activePlan.mutator().getLog();
      if (root) {
        log.write("Non-updated root reference ", ref);
      } else {
        log.write("Non-updated reference ", ref);
      }
      log.writeln(" -> ", RegionSpace.ForwardingWord.getForwardedObject(ref));
      VM.assertions.fail("Non-updated reference");
    }
    VM.assertions._assert(VM.debugging.validRef(ref));
    return traceObject(ref);
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(G1.G1, object)) {
      return G1.regionSpace.traceMarkObject(this, object);
    }
    return super.traceObject(object);
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(G1.G1, object)) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
