package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;

public class MergeCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {
   public static final int COMMAND_ID = 67;

   private Object value;
   private BiFunction remappingFunction;
   private Metadata metadata;
   private CacheNotifier<Object, Object> notifier;

   public MergeCommand(){
   }

   public MergeCommand(Object key, Object value,
                       BiFunction remappingFunction,
                       long flagsBitSet,
                       CommandInvocationId commandInvocationId,
                       Metadata metadata,
                       CacheNotifier notifier) {

      super(key, flagsBitSet, commandInvocationId);
      this.value = value;
      this.remappingFunction = remappingFunction;
      this.metadata = metadata;
      this.notifier = notifier;
   }

   public void init(CacheNotifier notifier) {
      //noinspection unchecked
      this.notifier = notifier;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // implementation not needed
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // implementation not needed (yet)
   }

   @Override
   public void initBackupWriteRcpCommand(BackupWriteRcpCommand command) {
      command.setMerge(commandInvocationId, key, value, remappingFunction, metadata, getFlagsBitSet(), getTopologyId());
   }

   @Override
   public void initPrimaryAck(PrimaryAckCommand command, Object localReturnValue) {
      command.initCommandInvocationIdAndTopologyId(commandInvocationId.getId(), getTopologyId());
      command.initWithReturnValue(true, localReturnValue);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      //noinspection unchecked
      MVCCEntry<Object, Object> e = (MVCCEntry) ctx.lookupEntry(key);

      if (e == null) {
         throw new IllegalStateException("Not wrapped");
      }

      Object oldValue = e.getValue();

      if (oldValue != null) {
         Object newValue = remappingFunction.apply(oldValue, value);
         if (newValue != null) {
            //replace the value
            notifier.notifyCacheEntryModified(key, newValue, metadata, oldValue, e.getMetadata(), true, ctx, this);
            e.setChanged(true);
            e.setValue(newValue);
            Metadatas.updateMetadata(e, metadata);
            return newValue;
         } else {
            // remove if newValue is null
            notifier.notifyCacheEntryRemoved(key, oldValue, e.getMetadata(), true, ctx, this);
            e.setRemoved(true);
            e.setValid(false);
            e.setChanged(true);
            e.setValue(null);
            return null;
         }
      } else {
         // When the value does not exist, just put it
         notifier.notifyCacheEntryCreated(key, value, metadata, true, ctx, this);
         e.setValue(value);
         e.setChanged(true);
         Metadatas.updateMetadata(e, metadata);
         if (e.isRemoved()) {
            e.setCreated(true);
            e.setExpired(false);
            e.setRemoved(false);
            e.setValid(true);
         }
         return value;
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      output.writeObject(remappingFunction);
      output.writeObject(metadata);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = input.readObject();
      remappingFunction = (BiFunction) input.readObject();
      metadata = (Metadata) input.readObject();
      commandInvocationId = CommandInvocationId.readFrom(input);
      setFlagsBitSet(input.readLong());
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitMergeCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.PRIMARY;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   public BiFunction getRemappingFunction() {
      return remappingFunction;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      MergeCommand that = (MergeCommand) o;

      if (!Objects.equals(metadata, that.metadata)) return false;
      if (!Objects.equals(value, that.value)) return false;
      return Objects.equals(remappingFunction, this.remappingFunction);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), value, remappingFunction, metadata);
   }

   @Override
   public String toString() {
      return "MergeCommand{" +
            "key=" + toStr(key) +
            ", value=" + toStr(value) +
            ", remappingFunction=" + toStr(remappingFunction) +
            ", metadata=" + metadata +
            ", flags=" + printFlags() +
            ", successful=" + isSuccessful() +
            ", valueMatcher=" + getValueMatcher() +
            ", topologyId=" + getTopologyId() +
            '}';
   }
}
