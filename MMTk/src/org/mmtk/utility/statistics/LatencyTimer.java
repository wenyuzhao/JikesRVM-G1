package org.mmtk.utility.statistics;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Phase;
import org.mmtk.utility.Log;
import org.mmtk.utility.options.EnableLatencyTimer;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
public class LatencyTimer {
//  public static final boolean ENABLED = true;//Options.enableLatencyTimer.getValue();
  public static final boolean LOG_COLLECTION_PHASE = false;
  public static final int LOG_MAX_THREADS = 7;
  public static final int MAX_THREADS = 1 << LOG_MAX_THREADS;
  public static final int LOG_MAX_EVENTS = 10;
  public static final int MAX_EVENTS = 1 << LOG_MAX_EVENTS;

  private static final int MAX_COLLECTION_PHASE_ID = 63 * 2;
  private static final int MUTATOR_PAUSE = 127;
  private static final int MUTATOR_RESUME = 128;
  private static final long[] data;// = new long[MAX_THREADS][MAX_LOG_SIZE][2];
  private static final int[] logCursor = new int[MAX_THREADS];
  private static final boolean[] mutatorKinds = new boolean[MAX_THREADS];
  private static long startTime, endTime;
  private static boolean loggingEnabled;

  @Inline
  public static boolean isEnabled() {
    return Options.enableLatencyTimer.getValue();
  }

  static {
//    Options.enableLatencyTimer = new EnableLatencyTimer();
    data = new long[MAX_THREADS * MAX_EVENTS * 2];
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(data != null);
    }
  }

  @Inline
  public static void start() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(startTime == 0);
      VM.assertions._assert(data != null);
      VM.assertions._assert(logCursor != null);
      VM.assertions._assert(mutatorKinds != null);
      Log.writeln("LatencyTimer Start");
    }
    loggingEnabled = true;
    startTime = VM.statistics.nanoTime();
  }

  @Inline
  private static void logEvent(int threadId, int eventId) { // 0..<64 are phases, 65: mutator pause, 66: mutator end
    if (!loggingEnabled || !isEnabled()) return;
    int cursor = logCursor[threadId]++;
    int base = threadId * (MAX_EVENTS * 2) + cursor * 2;//threadId << (LOG_MAX_EVENTS + 1) + cursor << 1;
    data[base + 0] = (long) eventId;
    data[base + 1] = VM.statistics.nanoTime();
  }

  @Inline
  public static void collectionPhaseStart(int threadId, short phaseId) {
    processMutator();
    logEvent(threadId, phaseId << 1);
  }

  @Inline
  public static void collectionPhaseEnd(int threadId, short phaseId) {
    processMutator();
    logEvent(threadId, (phaseId << 1) + 1);
  }

  @Inline
  public static void processMutator() {
    if (!loggingEnabled || !isEnabled()) return;
    processMutator(VM.activePlan.mutator().getId());
  }

  @Inline
  public static void processMutator(int mutatorId) {
    if (!loggingEnabled || !isEnabled()) return;
    mutatorKinds[mutatorId] = !VM.activePlan.isMutator();
  }

  @Inline
  public static void mutatorPause(int mutatorId) {
    processMutator();
    logEvent(mutatorId, MUTATOR_PAUSE);
  }

  @Inline
  public static void mutatorResume(int mutatorId) {
    processMutator();
    logEvent(mutatorId, MUTATOR_RESUME);
  }

  @Inline
  public static void stop() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(endTime == 0);
      Log.writeln("LatencyTimer Stop");
    }
    loggingEnabled = false;
    endTime = VM.statistics.nanoTime();
  }

  @Inline
  public static void dump() {
    Log.writeln("===== LatencyTimer Logs =====");
    for (int threadId = 0; threadId < MAX_THREADS; threadId++) {
      if (logCursor[threadId] == 0) continue;

      if (mutatorKinds[threadId]) {
        Log.write("=== Collector #", threadId);
        Log.writeln(" ===");
      } else {
        Log.write("=== Mutator #", threadId);
        Log.writeln(" ===");
      }

      for (int i = 0; i < logCursor[threadId]; i++) {
        int base = threadId * (MAX_EVENTS * 2) + i * 2;
        int eventId = (int) data[base + 0];
        long time = data[base + 1];
        Log.write(time);

        if (eventId == MUTATOR_PAUSE) {
          Log.writeln(" mutator pause");
        } else if (eventId == MUTATOR_RESUME) {
          Log.writeln(" mutator resume");
        } else if (eventId <= MAX_COLLECTION_PHASE_ID) {
          if (eventId % 2 == 0) {
            Log.write(" ");
            Log.write(Phase.getName((short) (eventId / 2)));
            Log.writeln(" start");
          } else if (eventId % 2 == 1) {
            Log.write(" ");
            Log.write(Phase.getName((short) ((eventId - 1) / 2)));
            Log.writeln(" end");
          }
        } else {
          Log.writeln(" unknown event id ", eventId);
        }
      }
    }
    Log.writeln("=============================");
  }
}
