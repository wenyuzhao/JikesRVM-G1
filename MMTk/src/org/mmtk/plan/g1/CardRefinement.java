package org.mmtk.plan.g1;

import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.Space;
import org.mmtk.policy.region.*;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class CardRefinement {

  @Uninterruptible
  public static abstract class LinearScan {
    public abstract void scan(Address card, ObjectReference object);
  }


  private static final LinearScan cardRefineLinearScan = new LinearScan() {
    @Uninterruptible @Inline public void scan(Address card, ObjectReference o) {
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
//        if (VM.VERIFY_ASSERTIONS) {
//          VM.assertions._assert(VM.debugging.validRef(field));
//        }
        Address remset = Region.getAddress(Region.of(field), Region.MD_REMSET);
        if (remset.isZero()) return;
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!remset.isZero());
        Address card = Card.of(source);
        RemSet.addCard(remset, card);
      }
    }
  };

  @Inline
  public static boolean refineOneCard(final Address card, boolean markDead) {
    if (CardTable.get(card) != Card.DIRTY) return false;
    CardTable.set(card, Card.NOT_DIRTY);
    Card.linearScan(card, cardRefineLinearScan, true);
    return true;
  }

  @Inline
  public static void collectorRefineAllDirtyCards(int id, int workers) {
    if (id == 0) {
//      let mut global_buffer = GLOBAL_RS_BUFFER.lock().unwrap();
//      global_buffer.clear();
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
}
