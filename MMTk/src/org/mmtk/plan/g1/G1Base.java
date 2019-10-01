package org.mmtk.plan.g1;

import org.mmtk.plan.Phase;
import org.mmtk.plan.StopTheWorld;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
class G1Base extends StopTheWorld {
  @Inline
  public static G1Constraints constraints() {
    return (G1Constraints) VM.activePlan.constraints();
  }
  // G1 Features
  public static final boolean ENABLE_CONCURRENT_MARKING = constraints().g1ConcurrentMarking();
  public static final boolean ENABLE_REMEMBERED_SETS = constraints().g1RememberedSets();

  // Collection phases
  public static final short EVACUATE_PREPARE = Phase.createSimple("evacuate-prepare");
  public static final short EVACUATE_CLOSURE = Phase.createSimple("evacuate-closure");
  public static final short EVACUATE_RELEASE = Phase.createSimple("evacuate-release");
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection");
  public static final short CLEANUP_BLOCKS = Phase.createSimple("cleanup-blocks");
  public static final short FLUSH_MUTATOR               = Phase.createSimple("flush-mutator", null);
  public static final short SET_BARRIER_ACTIVE          = Phase.createSimple("set-barrier", null);
  public static final short FLUSH_COLLECTOR             = Phase.createSimple("flush-collector", null);
  public static final short CLEAR_BARRIER_ACTIVE        = Phase.createSimple("clear-barrier", null);
  public static final short FINAL_MARK                  = Phase.createSimple("final-mark", null);
  public static final short REFINE_CARDS = Phase.createSimple("refine-cards");
  public static final short REMSET_ROOTS = Phase.createSimple("refine-cards");

  protected static final short preemptConcurrentClosure = Phase.createComplex("preeempt-concurrent-trace", null,
      Phase.scheduleMutator  (FLUSH_MUTATOR),
      Phase.scheduleCollector(CLOSURE)
  );

  public static final short CONCURRENT_CLOSURE = Phase.createConcurrent("concurrent-closure",
      Phase.scheduleComplex(preemptConcurrentClosure)
  );

  protected static final short concurrentClosure = Phase.createComplex("concurrent-mark", null,
      Phase.scheduleGlobal    (SET_BARRIER_ACTIVE),
      Phase.scheduleMutator   (SET_BARRIER_ACTIVE),
      Phase.scheduleCollector (FLUSH_COLLECTOR),
      Phase.scheduleConcurrent(CONCURRENT_CLOSURE),
      Phase.scheduleGlobal    (CLEAR_BARRIER_ACTIVE),
      Phase.scheduleMutator   (CLEAR_BARRIER_ACTIVE),
      Phase.scheduleCollector (FINAL_MARK)
  );

  public static final short fullTraceEvacuatePhase = Phase.createComplex("evacuate", null,
      Phase.scheduleMutator  (EVACUATE_PREPARE),
      Phase.scheduleGlobal   (EVACUATE_PREPARE),
      Phase.scheduleCollector(EVACUATE_PREPARE),
      // Roots
      Phase.scheduleMutator  (PREPARE_STACKS),
      Phase.scheduleGlobal   (PREPARE_STACKS),
      Phase.scheduleCollector(STACK_ROOTS),
      Phase.scheduleGlobal   (STACK_ROOTS),
      Phase.scheduleCollector(ROOTS),
      Phase.scheduleGlobal   (ROOTS),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),
      // Refs
      Phase.scheduleCollector(SOFT_REFS),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),
      Phase.scheduleCollector(WEAK_REFS),
      Phase.scheduleCollector(FINALIZABLE),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),
      Phase.scheduleCollector(PHANTOM_REFS),

      Phase.scheduleMutator  (EVACUATE_RELEASE),
      Phase.scheduleCollector(EVACUATE_RELEASE),
      Phase.scheduleGlobal   (EVACUATE_RELEASE)
  );

  public static final short remsetEvacuatePhase = Phase.createComplex("evacuate", null,
      Phase.scheduleMutator  (EVACUATE_PREPARE),
      Phase.scheduleGlobal   (EVACUATE_PREPARE),
      Phase.scheduleCollector(EVACUATE_PREPARE),
      // Roots
      Phase.scheduleMutator  (PREPARE_STACKS),
      Phase.scheduleGlobal   (PREPARE_STACKS),
      Phase.scheduleCollector(STACK_ROOTS),
      Phase.scheduleGlobal   (STACK_ROOTS),
      Phase.scheduleCollector(ROOTS),
      Phase.scheduleGlobal   (ROOTS),
      Phase.scheduleMutator  (REFINE_CARDS),
      Phase.scheduleCollector(REFINE_CARDS),
      Phase.scheduleCollector(REMSET_ROOTS),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),
      // Refs
      Phase.scheduleCollector(SOFT_REFS),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),
      Phase.scheduleCollector(WEAK_REFS),
      Phase.scheduleCollector(FINALIZABLE),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),
      Phase.scheduleCollector(PHANTOM_REFS),

      Phase.scheduleMutator  (REFINE_CARDS),
      Phase.scheduleCollector(REFINE_CARDS),
      Phase.scheduleCollector(REMSET_ROOTS),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),

      Phase.scheduleMutator  (EVACUATE_RELEASE),
      Phase.scheduleCollector(EVACUATE_RELEASE),
      Phase.scheduleGlobal   (EVACUATE_RELEASE)
  );

  public static short _collection = Phase.createComplex("_collection", null,
      Phase.scheduleComplex  (initPhase),
      // Mark
      Phase.scheduleComplex  (rootClosurePhase),
      Phase.scheduleComplex  (refTypeClosurePhase),
      Phase.scheduleComplex  (completeClosurePhase),
      // Select relocation sets
      Phase.scheduleGlobal   (RELOCATION_SET_SELECTION),
      // Evacuate
      Phase.scheduleComplex  (ENABLE_REMEMBERED_SETS ? remsetEvacuatePhase : fullTraceEvacuatePhase),
      // Cleanup
      Phase.scheduleCollector(CLEANUP_BLOCKS),

      Validation.scheduledPhase(),

      Phase.scheduleComplex  (finishPhase)
  );

  @Inline
  short getCollection() {
    return _collection;
  }
}
