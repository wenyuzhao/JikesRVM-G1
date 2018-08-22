package org.mmtk.utility;

import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class Atomic {
  @Uninterruptible
  public static class Int {
    private final int[] value = new int[] { 0 };

    @Inline
    public final void set(int v) {
      value[0] = v;
    }

    @Inline
    public final int get() {
      return value[0];
    }

    @Inline
    public final boolean attempt(int oldValue, int newValue) {
      Address pointer = ObjectReference.fromObject(value).toAddress();
      return pointer.attempt(oldValue, newValue);
    }

    @Inline
    public final int add(int value) {
      int oldValue, newValue;
      do {
        oldValue = get();
        newValue = oldValue + value;
      } while (!attempt(oldValue, newValue));
      return oldValue;
    }
  }
}
