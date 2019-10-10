package org.mmtk.plan.g1;

import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;

import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class CardQueue {
  private Address buffer = Address.zero();
  private int index = 0;
  private int size = 0;

  @Inline
  public void enqeueueNonAtomic(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!card.isZero());
    if (index < size) {
      // Fast path
      buffer.plus(index << LOG_BYTES_IN_ADDRESS).store(card);
      index += 1;
    } else {
      // Slow path
      enqeueueSlow(card);
    }
  }

  @NoInline
  private void enqeueueSlow(Address card) {
//    Log.writeln("[Hot Card Queue Resize]");
    // Expand space
    size = size == 0 ? 1024 : (size << 1);
    final int pages = (size << LOG_BYTES_IN_ADDRESS) >>> LOG_BYTES_IN_PAGE;
    Address newBuffer = G1.metaDataSpace.acquire(pages);
    // Copy
    for (int i = 0; i < index; i++) {
      final int offset = i << LOG_BYTES_IN_ADDRESS;
      Address v = buffer.plus(offset).loadAddress();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!v.isZero());
      newBuffer.plus(offset).store(v);
    }
    // Update data
    buffer = newBuffer;
    enqeueueNonAtomic(card);
  }

  @Inline
  public void reset() {
    index = 0;
  }

  @Inline
  public void collectorProcessAll(int id, int workers) {
    int size = (index + workers - 1) / workers;
    int startIndex = size * id;
    int _limit = size * (id + 1);
    int limitIndex = _limit > index ? index : _limit;
    for (int i = startIndex; i < limitIndex; i++) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(i < index);
      Address card = buffer.plus(i << LOG_BYTES_IN_ADDRESS).loadAddress();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!card.isZero());
      CardRefinement.refineOneCard(card, false);
    }
  }
}
