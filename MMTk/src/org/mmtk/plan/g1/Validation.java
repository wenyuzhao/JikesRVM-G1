package org.mmtk.plan.g1;

import org.mmtk.plan.Phase;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Space;
import org.mmtk.policy.region.Card;
import org.mmtk.policy.region.CardTable;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.RemSet;
import org.mmtk.utility.Log;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;

@Uninterruptible
public class Validation {
  static final boolean ENABLED = false;

  private static void validateEdge(ObjectReference src, Address slot, ObjectReference object) {
    if (object.isNull()) return;
    if (Space.isInSpace(G1.REGION_SPACE, object)) {
//      VM.assertions._assert(Region.getBool(Region.of(object), Region.MD_ALLOCATED));
      if (Region.getBool(Region.of(object), Region.MD_RELOCATE)) {
        VM.objectModel.dumpObject(src);
        VM.objectModel.dumpObject(object);
        Log.writeln(Space.getSpaceForObject(src).getName());
        Log.writeln(Space.getSpaceForObject(object).getName());
        Address remset = Region.getAddress(Region.of(object), Region.MD_REMSET);
        VM.assertions._assert(!remset.isZero());
        Log.writeln("card ", Card.of(src));
        Log.writeln("card mark ", CardTable.get(Card.of(src)));
        Log.writeln("region ", Region.of(object));
        if (RemSet.containsCard(remset, Card.of(src))) Log.writeln("Remset is correct");
        else Log.writeln("Remset is incorrect");
      }
      VM.assertions._assert(!Region.getBool(Region.of(object), Region.MD_RELOCATE));
    }
    VM.assertions._assert(Space.isMappedObject(object));
  }

  @Uninterruptible
  private static class Validator {
    void validateObject(ObjectReference object) {
      if (Space.isInSpace(G1.REGION_SPACE, object)) {
        VM.assertions._assert(Region.getBool(Region.of(object), Region.MD_ALLOCATED));
        if (Region.getBool(Region.of(object), Region.MD_RELOCATE)) {
          VM.objectModel.dumpObject(object);
          Log.writeln(Space.getSpaceForObject(object).getName());
        }
        VM.assertions._assert(!Region.getBool(Region.of(object), Region.MD_RELOCATE));
      }
      VM.assertions._assert(Space.isMappedObject(object));
    }
  }

  private static final Trace trace = new Trace(G1.metaDataSpace);
  private static final Validator validator = new Validator();
  private static final Offset GC_HEADER_OFFSET = VM.objectModel.GC_HEADER_OFFSET();
  public static final short VALIDATE_PLACEHOLDER = Phase.createSimple("validate-placeholder");
  public static final short VALIDATE_PREPARE = Phase.createSimple("validate-prepare");
  public static final short VALIDATE_CLOSURE = Phase.createSimple("validate-closure");
  public static final short VALIDATE_RELEASE = Phase.createSimple("validate-release");
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
      super(G1.SCAN_VALIDATE, trace);
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
      return object.toAddress().loadInt(GC_HEADER_OFFSET) == markState;
    }

    @Override
    public ObjectReference traceObject(ObjectReference object) {
      if (object.isNull()) return object;
      Address markWord = object.toAddress().plus(GC_HEADER_OFFSET);
      validator.validateObject(object);
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
