package org.mmtk.plan.g1;

import org.mmtk.plan.Phase;
import org.mmtk.plan.Plan;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.Space;
import org.mmtk.policy.region.*;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Conversions;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class CardRefinement {

  private static final Card.LinearScan cardRefineLinearScan = new Card.LinearScan<Object>() {
    @Uninterruptible @Inline public void scan(Address card, ObjectReference o, Object _context) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(VM.objectModel.objectStartRef(o).GE(card));
        VM.assertions._assert(VM.objectModel.objectStartRef(o).LT(card.plus(Card.BYTES_IN_CARD)));
        VM.assertions._assert(VM.debugging.validRef(o));
      }
      VM.scanning.scanObject(cardRefineTransitiveClosure, o);
    }
  };

  private static final TransitiveClosure cardRefineTransitiveClosure = new TransitiveClosure() {
    @Uninterruptible @Inline public void processEdge(ObjectReference source, Address slot) {
      ObjectReference field = slot.loadObjectReference();
      // obj.slot -> field
      if (RegionSpace.isCrossRegionRef(source, slot, field) && Space.isInSpace(G1.REGION_SPACE, field)) {
        Address region = Region.of(field);
        Address remset = Region.getAddress(region, Region.MD_REMSET);
        if (remset.isZero()) return;
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!remset.isZero());
        Address card = Card.of(source);
        RemSet.addCard(region, card);
      }
    }
  };

  @Inline
  public static boolean refineOneCard(final Address card, boolean markDead) {
    if (CardTable.get(card) != Card.DIRTY) return false;
    CardTable.set(card, Card.NOT_DIRTY);
    Card.linearScan(card, cardRefineLinearScan, false, null);
    return true;
  }

  @Inline
  public static void collectorRefineAllDirtyCards(int id, int workers) {
    if (id == 0) {
      Queue.releaseAll();
    }
//    int start_time = ::std::time::SystemTime::now();
    int size = (Card.CARDS_IN_HEAP + workers - 1) / workers;
    int start = size * id;
    int _limit = size * (id + 1);
    int limit = _limit > Card.CARDS_IN_HEAP ? Card.CARDS_IN_HEAP : _limit;
    int cards = 0;
    for (int i = start; i < limit; i++) {
      Address card = VM.HEAP_START.plus(i << Card.LOG_BYTES_IN_CARD);
      if (CardTable.get(card) == Card.DIRTY) {
        if (refineOneCard(card, false)) {
          cards += 1;
        }
      }
    }
//    let time = start_time.elapsed().unwrap().as_millis() as usize;
//    PLAN.predictor.timer.report_dirty_card_scanning_time(time, cards);
  }

  public static final int LOCAL_BUFFER_SIZE = 4096 - Constants.BYTES_IN_ADDRESS;
  private static final int PAGES_IN_BUFFER = 1 + (((LOCAL_BUFFER_SIZE + Constants.BYTES_IN_ADDRESS) - 1) / Constants.BYTES_IN_PAGE);

  @Uninterruptible
  public static class Queue {
    private static final Lock lock = VM.newLock("Global queue lock");
    private static Address head = Address.zero();
    private static int size = 0;

    @Inline
    public static Address allocateLocalQueue() {
      Address a = G1.metaDataSpace.acquire(PAGES_IN_BUFFER);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!a.isZero());
      return a.plus(Constants.BYTES_IN_ADDRESS);
    }

    @Inline
    public static void enqueue(Address buffer) {
//      release(buffer);
//      if (Plan.gcInProgress()) release(buffer);
      lock.acquire();
      buffer = Conversions.pageAlign(buffer);
      buffer.store(head);
      head = buffer;
      size += 1;
      if (size > 5) {
        ConcurrentRefinementWorker.GROUP.triggerCycle();
      }
      lock.release();
    }

    public static Address dequeue() {
      lock.acquire();
      Address buf = head;
      if (buf.isZero()) {
        lock.release();
        return Address.zero();
      }
      head = buf.loadAddress();
      size -= 1;
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(size >= 0);
      lock.release();
      return buf.plus(Constants.BYTES_IN_ADDRESS);
    }

    @Inline
    public static void release(Address buffer) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!buffer.isZero());
      G1.metaDataSpace.release(Conversions.pageAlign(buffer));
    }

    public static void releaseAll() {
      lock.acquire();
      Address buf = head;
      while (!buf.isZero()) {
        Address next = buf.loadAddress();
        release(buf);
        buf = next;
      }
      head = Address.zero();
      size = 0;
      lock.release();
    }
  }
}
