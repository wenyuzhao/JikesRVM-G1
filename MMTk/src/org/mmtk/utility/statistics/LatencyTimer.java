package org.mmtk.utility.statistics;

import org.mmtk.plan.MutatorContext;
import org.mmtk.plan.Phase;
import org.mmtk.utility.Log;
import org.mmtk.utility.options.EnableLatencyTimer;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;

import java.util.Arrays;

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
  private static final boolean[] loggedMutator = new boolean[MAX_THREADS];
//  public static Counter latency = new EventCounter("gc.full");
//  private static final boolean[] mutatorKinds = new boolean[MAX_THREADS];
//  private static long startTime, endTime;
  private static boolean loggingEnabled = false;

  @Inline
  public static boolean isEnabled() {
//    return true;//
    return VM.activePlan.constraints().__g1LatencyTimer();
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
   loggingEnabled = true;
//    startTime = VM.statistics.nanoTime();
  }

  @Inline
  public static void enableLogging() {
    // loggingEnabled = true;
  }

  @Inline
  public static void disableLogging() {
    // loggingEnabled = false;
  }

  static long lastStartTime = -1;

  @Inline
  public static void startPausingMutators() {
    lastStartTime = VM.statistics.nanoTime();
  }

  @Inline
  public static void allMutatorsResumed() {
//    lastStartTime = -1;
  }
//    for (int threadId = 0; threadId < MAX_THREADS; threadId++) {
//      if (!loggedMutator[threadId]) continue;
//      loggedMutator[threadId] = false;
//
//      int cursor = logCursor[threadId];
//      if (cursor == 0) continue;
//      int base = threadId * (MAX_EVENTS * 2) + cursor * 2;
//      int eventId = (int) data[base + 0];
//
//      if (eventId == MUTATOR_PAUSE) {
//        long time = data[base + 1];
//        if (time > startTime) {
//          logEvent(MUTATOR_RESUME, eventId);
//        } else {
//          logCursor[threadId] -= 1;
//        }
//      }
//    }

  @Inline
  private static void logEvent(int threadId, int eventId) { // 0..<64 are phases, 65: mutator pause, 66: mutator end
//    if (!loggingEnabled || !isEnabled()) return;
    int cursor = logCursor[threadId]++;
    int base = threadId * (MAX_EVENTS * 2) + cursor * 2;//threadId << (LOG_MAX_EVENTS + 1) + cursor << 1;
    data[base + 0] = (long) eventId;
    data[base + 1] = VM.statistics.nanoTime();
  }

  @Inline
  public static void mutatorPause(int mutatorId) {
    if (!loggingEnabled || !isEnabled()) return;
    logEvent(mutatorId, MUTATOR_PAUSE);
//    loggedMutator[mutatorId] = true;
  }

  @Inline
  public static void mutatorResume(int mutatorId) {
    if (!isEnabled()) return;
    int cursor = logCursor[mutatorId] - 1;
    if (cursor < 0) return;
    int base = mutatorId * (MAX_EVENTS * 2) + cursor * 2;//threadId << (LOG_MAX_EVENTS + 1) + cursor << 1;
    if (data[base + 0] == MUTATOR_PAUSE) {
      if (data[base + 1] <= lastStartTime) {
        logCursor[mutatorId]--;
      } else {
        logEvent(mutatorId, MUTATOR_RESUME);
      }
    } else {
      logCursor[mutatorId]--;
    }
  }

//  static LongCounter2 counterMin = new LongCounter2("pause.min", true, true);
//  static LongCounter2 counterMax = new LongCounter2("pause.max", true, true);
//  static LongCounter2 counterMid = new LongCounter2("pause.mid", true, true);
//  static LongCounter2 counterAvg = new LongCounter2("pause.avg", true, true);
//  static LongCounter2 counter95  = new LongCounter2("pause.95", true, true);

  @Inline
  @Interruptible
  public static void stop() {
    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(endTime == 0);
      Log.writeln("LatencyTimer Stop");
    }
    loggingEnabled = false;
//    endTime = VM.statistics.nanoTime();

//    long[] pauses = new long[10000];
//    int pausesCursor = 0;
//    for (int threadId = 0; threadId < MAX_THREADS; threadId++) {
//      if (logCursor[threadId] == 0) continue;
//
//      long pauseTime = -1;
//      for (int i = 0; i < logCursor[threadId]; i++) {
//        int base = threadId * (MAX_EVENTS * 2) + i * 2;
//        int eventId = (int) data[base + 0];
//        long time = data[base + 1];
////        Log.write(time);
//
//        if (eventId == MUTATOR_PAUSE) {
//          pauseTime = time;
//        } else if (eventId == MUTATOR_RESUME) {
//          if (pauseTime == -1 || time <= pauseTime) continue;
////          log.writeln(time - pauseTime);
//          pauses[pausesCursor++] = time - pauseTime;
//          pauseTime = -1;
//        } else {
//          VM.assertions.fail("Unimplemented");
//        }
//      }
//    }
//
//    long[] pauses2 = new long[pausesCursor];
//    for (int i = 0; i < pausesCursor; i++)
//      pauses2[i] = pauses[i];
//    Arrays.sort(pauses2);
//    counterMin.log(pauses2[0]);
//    counterMax.log(pauses2[pausesCursor - 1]);
//    counterAvg.log(mean(pauses2));
//    counterMid.log(pauses2[pausesCursor / 2]);
//    counter95.log(pauses2[(int) (pausesCursor * 0.95)]);
//    Arrays.mea
  }

  public static long mean(long[] m) {
    long sum = 0;
    for (int i = 0; i < m.length; i++) {
      sum += m[i];
    }
    return sum / m.length;
  }

  @Inline
  public static void dump() {
    Log log = VM.activePlan.mutator().getLog();
    log.writeln("===== LatencyTimer Pause Times =====");
//    Log.writeln("PauseTime(ns)");
    for (int threadId = 0; threadId < MAX_THREADS; threadId++) {
      if (logCursor[threadId] == 0) continue;

//      if (mutatorKinds[threadId]) {
//        Log.write("=== Collector #", threadId);
//        Log.writeln(" ===");
//      } else {
//        Log.write("=== Mutator #", threadId);
//        Log.writeln(" ===");
//      }


      long pauseTime = -1;
      for (int i = 0; i < logCursor[threadId]; i++) {
        int base = threadId * (MAX_EVENTS * 2) + i * 2;
        int eventId = (int) data[base + 0];
        long time = data[base + 1];
//        Log.write(time);

        if (eventId == MUTATOR_PAUSE) {
          pauseTime = time;
        } else if (eventId == MUTATOR_RESUME) {
          if (pauseTime == -1 || time < pauseTime) continue;
          log.writeln(time - pauseTime);
          pauseTime = -1;
        } else {
          VM.assertions.fail("Unimplemented");
        }
      }
    }
    log.writeln("===== LatencyTimer Pause Times End =====");
  }
}
