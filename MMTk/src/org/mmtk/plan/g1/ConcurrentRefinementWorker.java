package org.mmtk.plan.g1;

import org.mmtk.plan.CollectorContext;
import org.mmtk.plan.ParallelCollector;
import org.mmtk.plan.ParallelCollectorGroup;
import org.mmtk.policy.region.CardTable;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.vm.Lock;
import org.mmtk.vm.Monitor;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.Address;

@Uninterruptible
@NonMoving
public class ConcurrentRefinementWorker extends ParallelCollector {
  public static final ParallelCollectorGroup GROUP = new ParallelCollectorGroup("card-refine");
  public static Monitor monitor;// = VM.newHeavyCondLock("x");
  public Lock lock = VM.newLock("cx");
  static boolean forceIdle = false;

  public static void pause() {
    forceIdle = true;
    GROUP.abortCycle();
    GROUP.waitForCycle();
  }

  public static void resume() {
    forceIdle = false;
  }

  @Interruptible
  public static void spawn() {
    monitor = VM.newHeavyCondLock("x");
    GROUP.initGroup(1, ConcurrentRefinementWorker.class);
  }

  @Override
  @Interruptible
  public void initCollector(int id) {
    super.initCollector(id);
  }


  @Override
  @Unpreemptible
  public void run() {
//    monitor.lock();
    while (true) {
//      lock.acquire();

//      monitor.await();
//      Log.writeln("Park");
      park();
//      Log.writeln("Refine");
      if (forceIdle) continue;
      refine();
    }
//    monitor.unlock();
  }

  private void refineOneBuffer(final Address buffer) {
    final Address limit = buffer.plus(CardRefinement.LOCAL_BUFFER_SIZE);
    for (Address cursor = buffer; cursor.LT(limit); cursor = cursor.plus(Constants.BYTES_IN_ADDRESS)) {
      if (GROUP.isAborted()) return;
      Address card = cursor.loadAddress();
      if (card.isZero()) return;
      if (G1.ENABLE_HOT_CARD_OPTIMIZATION && CardTable.increaseHotness(card)) {
        // Skip this hot card
        continue;
      }
      CardRefinement.refineOneCard(card, false);
    }
  }

  private void refine() {
//    Log.writeln("Refine");
    while (!GROUP.isAborted()) {
      Address buf = CardRefinement.Queue.dequeue();
      if (buf.isZero()) return;
      refineOneBuffer(buf);
      CardRefinement.Queue.release(buf);
    }
  }
}
