package org.mmtk.plan.g1;

import org.mmtk.plan.Phase;
import org.mmtk.plan.Trace;
import org.mmtk.policy.Space;
import org.mmtk.policy.region.Card;
import org.mmtk.policy.region.CardTable;
import org.mmtk.policy.region.Region;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;

@Uninterruptible
public class Validation {
  static final boolean ENABLED = false;

  public static void validateEdge(ObjectReference src, Address slot, ObjectReference object) {
    if (!VM.VERIFY_ASSERTIONS || object.isNull()) return;
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
//      VM.assertions._assert(Region.getBool(Region.of(object), Region.MD_ALLOCATED));
      if (!Region.getBool(Region.of(object), Region.MD_ALLOCATED)) {
        VM.objectModel.dumpObject(src);
        Log.writeln(Space.getSpaceForObject(src).getName());
        Log.writeln("src card ", Card.of(src));
        Log.writeln("src card mark ", CardTable.get(Card.of(src)));
        Log.writeln("slot ", slot);

        VM.objectModel.dumpObject(object);
        Log.writeln(Space.getSpaceForObject(object).getName());
        Address remset = Region.getAddress(Region.of(object), Region.MD_REMSET);
        VM.assertions._assert(!remset.isZero());
        Log.writeln("region ", Region.of(object));
//        if (RemSet.containsCard(remset, Card.of(src))) Log.writeln("Remset is correct");
//        else Log.writeln("Remset is incorrect");
        VM.assertions.fail("");
      }
      if (Region.getBool(Region.of(object), Region.MD_RELOCATE)) {
        VM.objectModel.dumpObject(src);
        VM.objectModel.dumpObject(object);
        Log.writeln(Space.getSpaceForObject(src).getName());
        Log.writeln(Space.getSpaceForObject(object).getName());

        Log.writeln("slot ", slot);
        Address remset = Region.getAddress(Region.of(object), Region.MD_REMSET);
        VM.assertions._assert(!remset.isZero());
        Log.writeln("card ", Card.of(src));
        Log.writeln("card mark ", CardTable.get(Card.of(src)));
        Log.writeln("region ", Region.of(object));
//        if (RemSet.containsCard(remset, Card.of(src))) Log.writeln("Remset is correct");
//        else Log.writeln("Remset is incorrect");
      }
      VM.assertions._assert(!Region.getBool(Region.of(object), Region.MD_RELOCATE));
    }
    if (!Space.isMappedObject(object)) {
      VM.objectModel.dumpObject(object);
      VM.objectModel.dumpObject(src);
      Log.writeln(Space.getSpaceForObject(src).getName());
      if (Space.isInSpace(G1.REGION_SPACE, src)) {
        Log.writeln(Region.getBool(Region.of(src), Region.MD_RELOCATE) ? "src in relocate region" : "src not in relocate region");
        Log.writeln(Region.getBool(Region.of(src), Region.MD_ALLOCATED) ? "region(src) is allocated" : "region(src) is not allocated");
      }
      Log.writeln("slot ", slot);
      Log.writeln("card(src) = ", Card.of(src));
      Log.writeln("card mark = ", CardTable.get(Card.of(src)));
      Log.writeln("region(object) = ", Region.of(object));
    }
    VM.assertions._assert(Space.isMappedObject(object));
  }

  public static void validateObject(ObjectReference object) {
    if (!VM.VERIFY_ASSERTIONS) return;
    VM.assertions._assert(Space.isMappedObject(object));
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
      VM.assertions._assert(Region.getBool(Region.of(object), Region.MD_ALLOCATED));
      if (Region.getBool(Region.of(object), Region.MD_RELOCATE)) {
        VM.objectModel.dumpObject(object);
        Log.writeln(Space.getSpaceForObject(object).getName());
      }
      VM.assertions._assert(!Region.getBool(Region.of(object), Region.MD_RELOCATE));
      VM.assertions._assert(G1.regionSpace.isLivePrev(object));
    } else if (Space.isInSpace(G1.LOS, object)) {
      VM.assertions._assert(G1.loSpace.isLive(object));
    } else if (Space.isInSpace(G1.IMMORTAL, object)) {
      VM.assertions._assert(G1.immortalSpace.isMarked(object));
    }
  }

  private static final Trace trace = new Trace(G1.metaDataSpace);
  private static final Offset GC_HEADER_OFFSET = VM.objectModel.GC_HEADER_OFFSET();
  public static final short VALIDATE_PLACEHOLDER = Phase.createSimple("validate-placeholder", null);
  public static final short VALIDATE_PREPARE = Phase.createSimple("validate-prepare", null);
  public static final short VALIDATE_CLOSURE = Phase.createSimple("validate-closure", null);
  public static final short VALIDATE_RELEASE = Phase.createSimple("validate-release", null);
  public static final short validatePhase = Phase.createComplex("validate", null,
      Phase.scheduleMutator  (VALIDATE_PREPARE),
      Phase.scheduleGlobal   (VALIDATE_PREPARE),
      Phase.scheduleCollector(VALIDATE_PREPARE),
      // Roots
      Phase.scheduleMutator  (G1.PREPARE_STACKS),
      Phase.scheduleGlobal   (G1.PREPARE_STACKS),
      Phase.scheduleCollector(G1.STACK_ROOTS),
      Phase.scheduleGlobal   (G1.STACK_ROOTS),
      Phase.scheduleCollector(G1.ROOTS),
      Phase.scheduleGlobal   (G1.ROOTS),
      Phase.scheduleCollector(VALIDATE_CLOSURE),
      // Refs
      Phase.scheduleCollector(G1.SOFT_REFS),
      Phase.scheduleCollector(VALIDATE_CLOSURE),
      Phase.scheduleCollector(G1.WEAK_REFS),
      Phase.scheduleCollector(G1.FINALIZABLE),
      Phase.scheduleCollector(VALIDATE_CLOSURE),
      Phase.scheduleCollector(G1.PHANTOM_REFS),

      Phase.scheduleMutator  (VALIDATE_RELEASE),
      Phase.scheduleCollector(VALIDATE_RELEASE),
      Phase.scheduleGlobal   (VALIDATE_RELEASE)
  );
  private static int markState = 0;

  static int scheduledPhase() {
    if (!ENABLED) return Phase.schedulePlaceholder(VALIDATE_PLACEHOLDER);
    return Phase.scheduleComplex(validatePhase);
  }

  static void prepare() {
    markState += 1;
    trace.prepare();
  }

  static void release() {
    trace.release();
  }

  @Uninterruptible
  static class TraceLocal extends org.mmtk.plan.TraceLocal {
    public TraceLocal() {
      super(trace);
    }

    @Override
    protected boolean overwriteReferenceDuringTrace() {
      return false;
    }

    @Override
    @Inline
    public void processEdge(ObjectReference source, Address slot) {
      validateEdge(source, slot, slot.loadObjectReference());
      super.processEdge(source, slot);
    }

    @Override
    public boolean isLive(ObjectReference object) {
      if (object.isNull()) return false;
      validateObject(object);
      return object.toAddress().loadInt(GC_HEADER_OFFSET) == markState;
    }

    @Override
    public ObjectReference traceObject(ObjectReference object) {
      if (object.isNull()) return object;
      Address markWord = object.toAddress().plus(GC_HEADER_OFFSET);
      validateObject(object);
      if (markWord.loadInt() != markState) {
        markWord.store(markState);
        processNode(object);
      }
      return object;
    }

    @Override
    public boolean willNotMoveInCurrentCollection(ObjectReference object) {
      return true;
    }
  }
}
