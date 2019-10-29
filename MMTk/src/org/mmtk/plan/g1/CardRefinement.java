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
    if (Space.isInSpace(G1.REGION_SPACE, card) && Region.getInt(Region.of(card), Region.MD_GENERATION) != Region.OLD) {
      return false;
    }
    Card.linearScan(card, cardRefineLinearScan, false, null);
    return true;
  }

  @Inline
  private static void refineOneBuffer(final Address buffer) {
    final Address limit = buffer.plus(CardRefinement.filledRSBufferQueue.LOCAL_BUFFER_SIZE);
    for (Address cursor = buffer; cursor.LT(limit); cursor = cursor.plus(Constants.BYTES_IN_ADDRESS)) {
      Address card = cursor.loadAddress();
      if (card.isZero()) return;
      CardRefinement.refineOneCard(card, false);
    }
  }

  @Inline
  public static void collectorRefineAllDirtyCards(int id, int workers) {
    long startTime = id == 0 ? VM.statistics.nanoTime() : 0;
    VM.activePlan.collector().rendezvous();

    // 1. Drain filledRSBufferQueue
    Address buffer;
    while (!(buffer = filledRSBufferQueue.dequeue()).isZero()) {
      refineOneBuffer(buffer);
      filledRSBufferQueue.release(buffer);
    }
    // 2. Drain cardQueue
    hotCardQueue.collectorProcessAll(id, workers);

    VM.activePlan.collector().rendezvous();
    if (id == 0) {
      hotCardQueue.reset();
      G1.predictor.stat.totalRefineTime += VM.statistics.nanoTime() - startTime;
    }
  }

  public static final BufferQueue filledRSBufferQueue = new BufferQueue();
  public static final CardQueue hotCardQueue = new CardQueue();
}
