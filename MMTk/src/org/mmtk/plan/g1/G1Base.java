package org.mmtk.plan.g1;

import org.mmtk.plan.Phase;
import org.mmtk.plan.PlanConstraints;
import org.mmtk.plan.StopTheWorld;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
class G1Base extends StopTheWorld {
  @Inline
  public static PlanConstraints constraints() {
    return VM.activePlan.constraints();
  }
  // G1 Features
  public static final boolean ENABLE_CONCURRENT_MARKING = constraints().g1ConcurrentMarking();
  public static final boolean ENABLE_REMEMBERED_SETS = constraints().g1RememberedSets();
  public static final boolean ENABLE_CONCURRENT_REFINEMENT = constraints().g1ConcurrentRefinement();
  public static final boolean ENABLE_HOT_CARD_OPTIMIZATION = constraints().g1HotCardOptimization();
  public static final boolean ENABLE_GENERATIONAL_GC = constraints().g1GenerationalGC();
  public static final boolean ENABLE_PAUSE_TIME_PREDICTOR = constraints().g1PauseTimePredictor();
  public static final boolean USE_XOR_BARRIER = constraints().g1UseXorBarrier();
  public static final boolean FORCE_DRAIN_MODBUF = constraints().g1ForceDrainModbuf();

  private static void requires(boolean x) {
    if (!x) VM.assertions.fail("");
  }

  static {
    // Verify features consistency
    if (ENABLE_CONCURRENT_REFINEMENT) requires(ENABLE_REMEMBERED_SETS);
    if (ENABLE_HOT_CARD_OPTIMIZATION) {
      requires(ENABLE_REMEMBERED_SETS);
      requires(ENABLE_CONCURRENT_REFINEMENT);
    }
    if (ENABLE_GENERATIONAL_GC) requires(ENABLE_REMEMBERED_SETS);
    if (ENABLE_PAUSE_TIME_PREDICTOR) requires(ENABLE_REMEMBERED_SETS);
  }



  // Collection phases
  public static final short EVACUATE_PREPARE         = Phase.createSimple("evacuate-prepare", null);
  public static final short EVACUATE_CLOSURE         = Phase.createSimple("evacuate-closure", null);
  public static final short EVACUATE_RELEASE         = Phase.createSimple("evacuate-release", null);
  public static final short RELOCATION_SET_SELECTION = Phase.createSimple("relocation-set-selection", null);
  public static final short FLUSH_MUTATOR            = Phase.createSimple("flush-mutator", null);
  public static final short SET_BARRIER_ACTIVE       = Phase.createSimple("set-barrier", null);
  public static final short FLUSH_COLLECTOR          = Phase.createSimple("flush-collector", null);
  public static final short CLEAR_BARRIER_ACTIVE     = Phase.createSimple("clear-barrier", null);
  public static final short FINAL_MARK               = Phase.createSimple("final-mark", null);
  public static final short REFINE_CARDS             = Phase.createSimple("refine-cards", null);
  public static final short REMSET_ROOTS             = Phase.createSimple("remset-roots", null);
  public static final short STAT_REMSET              = Phase.createSimple("stat-remset", null);

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

  public static final short fullTraceEvacuatePhase = Phase.createComplex("evacuate",
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

  public static final short remsetEvacuatePhase = Phase.createComplex("evacuate",
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
      Phase.scheduleGlobal   (REFINE_CARDS),
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

      Phase.scheduleGlobal   (REFINE_CARDS),
      Phase.scheduleMutator  (REFINE_CARDS),
      Phase.scheduleCollector(REFINE_CARDS),
      Phase.scheduleCollector(REMSET_ROOTS),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),

      Phase.scheduleMutator  (EVACUATE_RELEASE),
      Phase.scheduleCollector(EVACUATE_RELEASE),
      Phase.scheduleGlobal   (EVACUATE_RELEASE)
  );

  public static final short mixedCollection = Phase.createComplex("mixed-collection", null,
      Phase.scheduleComplex  (initPhase),
      // Mark
      Phase.scheduleComplex  (rootClosurePhase),
      Phase.scheduleComplex  (refTypeClosurePhase),
      Phase.scheduleComplex  (completeClosurePhase),
      // Select relocation sets
      Phase.scheduleGlobal   (RELOCATION_SET_SELECTION),
      Phase.schedulePlaceholder(STAT_REMSET),
      // Evacuate
      Phase.scheduleComplex  (ENABLE_REMEMBERED_SETS ? remsetEvacuatePhase : fullTraceEvacuatePhase),

      Validation.scheduledPhase(),

      Phase.scheduleComplex  (finishPhase)
  );

  public static final short nurseryCollection = Phase.createComplex("nursery-collection", null,
      Phase.scheduleComplex  (initPhase),
      // Select relocation sets
      Phase.scheduleGlobal   (RELOCATION_SET_SELECTION),
      Phase.schedulePlaceholder(STAT_REMSET),
      // Evacuate
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
      Phase.scheduleGlobal   (REFINE_CARDS),
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

      Phase.scheduleGlobal   (REFINE_CARDS),
      Phase.scheduleMutator  (REFINE_CARDS),
      Phase.scheduleCollector(REFINE_CARDS),
      Phase.scheduleCollector(REMSET_ROOTS),
      Phase.scheduleGlobal   (EVACUATE_CLOSURE),
      Phase.scheduleCollector(EVACUATE_CLOSURE),

      Phase.scheduleMutator  (EVACUATE_RELEASE),
      Phase.scheduleCollector(EVACUATE_RELEASE),
      Phase.scheduleGlobal   (EVACUATE_RELEASE),

      Validation.scheduledPhase(),

      Phase.scheduleComplex  (finishPhase)
  );

  @Inline
  short getCollection() {
    if (VM.VERIFY_ASSERTIONS) {
      if (G1.gcKind == G1.GCKind.YOUNG) {
        VM.assertions._assert(ENABLE_GENERATIONAL_GC);
      }
    }
    return G1.gcKind == G1.GCKind.YOUNG ? nurseryCollection : mixedCollection;
  }
}
