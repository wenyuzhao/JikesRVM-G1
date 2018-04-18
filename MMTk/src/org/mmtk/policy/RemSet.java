package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.utility.Constants;
import org.mmtk.utility.SimpleHashtable;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;

@Uninterruptible
public class RemSet {
  public static Extent EXTENT;
  public static int ENTRIES; // Entries per RemSet
  public static Extent EXTENT_PER_ENTRY; // Entries per RemSet
  public static SimpleHashtable hashtable;
  // type RSet = Map<Block, Set<CardIndex>>
  // type RSetMap = Map<Block, RSet>

  /** A bitmap of all cards in a block */
  @Uninterruptible
  public static class CardSet {
    public static final int CARD_SIZE = CardTable.BYTES_IN_CARD;
    public static Extent EXTENT;

    public static Address addCard(Address block) {

    }
  }

  /* RemSet of a block */
  public static Address of(Address block) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.isAligned(block));
    int index = MarkBlock.indexOf(block);
    Address remSetsStart = EmbeddedMetaData.getMetaDataBase(block);
    return remSetsStart.plus(EXTENT.toInt() * index);
  }

  private static boolean loadBit(Address start, int index) {
    int offset = index / 8;
    start = start.plus(offset);
    index -= offset * 8;
    byte b = start.loadByte();
    return (b & ((byte) (1 << (7 - index)))) > 0;
  }

  public static void addCard(Address block, Address card) {
    if (!hashtable.isValid()) hashtable.acquireTable();
    Address remSet = hashtable.getPayloadAddress(hashtable.getEntry(block.toWord(), true));
    int cardIndexInBlock = card
  }
  public static void removeCard(Address block, Address card) {

  }

  private static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  static {
    MarkBlock.setAdditionalMetadataPagesPerRegion(MarkBlock.METADATA_PAGES_PER_REGION - MarkBlock.USED_METADATA_PAGES_PER_REGION);

    int memorySize = VM.AVAILABLE_END.diff(VM.AVAILABLE_START).toInt();
    int maxBlocks = ceilDiv(memorySize, MarkBlock.BYTES_IN_BLOCK);
    int logBlocks;
    for (logBlocks = 0; (1 << logBlocks) < maxBlocks; logBlocks++);

    int cardsInBlock = MarkBlock.BYTES_IN_BLOCK / CardTable.BYTES_IN_CARD;
    CardSet.EXTENT = Extent.fromIntZeroExtend(cardsInBlock / Constants.BITS_IN_BYTE);
    EXTENT = Extent.fromIntZeroExtend(CardSet.EXTENT.toInt() * maxBlocks);   //Extent.fromIntZeroExtend(MarkBlock.ADDITIONAL_METADATA.toInt() / MarkBlock.BLOCKS_IN_REGION);
    /*
    CardSet.EXTENT = Extent.fromIntZeroExtend(MarkBlock.BYTES_IN_BLOCK / CardSet.CARD_SIZE);
    EXTENT_PER_ENTRY = CardSet.EXTENT.plus(Constants.BYTES_IN_ADDRESS);
    ENTRIES = EXTENT.toInt() / EXTENT_PER_ENTRY.toInt();
    */
    hashtable = new SimpleHashtable(Plan.metaDataSpace, logBlocks, EXTENT) {};
  }
}
