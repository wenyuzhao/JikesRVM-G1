package org.mmtk.plan.g1;

import org.mmtk.plan.Phase;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldCollector;
import org.mmtk.plan.TraceLocal;
import org.mmtk.utility.Log;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.pragma.Unpreemptible;

@Uninterruptible
public class G1CollectorBase extends StopTheWorldCollector {
  @Override
  @Unpreemptible
  public void run() {
    while (true) {
      park();
      if (Plan.parallelWorkers.isMember(this)) {
        collect();
      } else {
        concurrentCollect();
      }
    }
  }

  protected static volatile boolean continueCollecting;

  /** Perform some concurrent garbage collection */
  @Unpreemptible
  public final void concurrentCollect() {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!Plan.gcInProgress());
    do {
      short phaseId = Phase.getConcurrentPhaseId();
      concurrentCollectionPhase(phaseId);
    } while (continueCollecting);
  }

  @Override
  public void collect() {
    if (!Phase.isPhaseStackEmpty()) {
      Phase.continuePhaseStack();
    } else {
      Phase.beginNewPhaseStack(Phase.scheduleComplex(global().getCollection()));
    }
  }

  /**
   * Perform some concurrent collection work.
   *
   * @param phaseId The unique phase identifier
   */
  @Unpreemptible
  public void concurrentCollectionPhase(short phaseId) {
    if (phaseId == G1.CONCURRENT_CLOSURE) {
      if (G1.VERBOSE) Log.writeln(Phase.getName(phaseId));
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!Plan.gcInProgress());
      }
      TraceLocal trace = getCurrentTrace();
      while (!trace.incrementalTrace(100)) {
        if (group.isAborted()) {
          trace.flush();
          break;
        }
      }
      if (rendezvous() == 0) {
        continueCollecting = false;
        if (!group.isAborted()) {
          /* We are responsible for ensuring termination. */
          if (Options.verbose.getValue() >= 2) Log.writeln("< requesting mutator flush >");
          VM.collection.requestMutatorFlush();

          if (Options.verbose.getValue() >= 2) Log.writeln("< mutators flushed >");

          if (concurrentTraceComplete()) {
            continueCollecting = Phase.notifyConcurrentPhaseComplete();
          } else {
            continueCollecting = true;
            Phase.notifyConcurrentPhaseIncomplete();
          }
        } else {
          G1.gcKind = G1.GCKind.FULL;
        }
      }
      rendezvous();
      return;
    }

    Log.write("Concurrent phase ");
    Log.write(Phase.getName(phaseId));
    Log.writeln(" not handled.");
    VM.assertions.fail("Concurrent phase not handled!");
  }

  /**
   * @return whether all work has been completed
   */
  protected boolean concurrentTraceComplete() {
    if (!global().markTrace.hasWork()) {
      return true;
    }
    return false;
  }

  @Inline
  static G1 global() {
    return (G1) VM.activePlan.global();
  }
}
