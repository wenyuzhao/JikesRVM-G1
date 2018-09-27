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
//  private static final boolean[] mutatorKinds = new boolean[MAX_THREADS];
//  private static long startTime, endTime;
  private static boolean loggingEnabled = false;

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
//      VM.assertions._assert(startTime == 0);
      VM.assertions._assert(data != null);
      VM.assertions._assert(logCursor != null);
//      VM.assertions._assert(mutatorKinds != null);
      Log.writeln("LatencyTimer Start");
    }
//    loggingEnabled = true;
//    startTime = VM.statistics.nanoTime();
  }

  @Inline
  public static void enableLogging() {
    loggingEnabled = true;
  }

  @Inline
  public static void disableLogging() {
    loggingEnabled = false;
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
  public static void mutatorPause(int mutatorId) {
    if (!loggingEnabled || !isEnabled()) return;
    logEvent(mutatorId, MUTATOR_PAUSE);
  }

  @Inline
  public static void mutatorResume(int mutatorId) {
    if (!loggingEnabled || !isEnabled()) return;
    logEvent(mutatorId, MUTATOR_RESUME);
  }

  @Inline
  public static void stop() {
    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(endTime == 0);
      Log.writeln("LatencyTimer Stop");
    }
    loggingEnabled = false;
//    endTime = VM.statistics.nanoTime();
  }

  @Inline
  public static void dump() {
    Log.writeln("===== LatencyTimer Pause Times =====");
    Log.writeln("PauseTime(ns)");
    for (int threadId = 0; threadId < MAX_THREADS; threadId++) {
      if (logCursor[threadId] == 0) continue;

//      if (mutatorKinds[threadId]) {
//        Log.write("=== Collector #", threadId);
//        Log.writeln(" ===");
//      } else {
//        Log.write("=== Mutator #", threadId);
//        Log.writeln(" ===");
//      }

      long pauseTime = -1l;
      for (int i = 0; i < logCursor[threadId]; i++) {
        int base = threadId * (MAX_EVENTS * 2) + i * 2;
        int eventId = (int) data[base + 0];
        long time = data[base + 1];
//        Log.write(time);

        if (eventId == MUTATOR_PAUSE) {
          pauseTime = time;
        } else if (eventId == MUTATOR_RESUME) {
          if (pauseTime == -1l || time < pauseTime) continue;
          Log.writeln(time - pauseTime);
          pauseTime = -1;
        } else {
          VM.assertions.fail("Unimplemented");
        }
      }
    }
    Log.writeln("===== LatencyTimer Pause Times End =====");
  }
}
