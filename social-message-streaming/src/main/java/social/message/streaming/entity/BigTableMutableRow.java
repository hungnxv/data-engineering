package social.message.streaming.entity;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.List;

public interface BigTableMutableRow extends Serializable {
    List<Mutation> toMutations(final Instant instantTime);

    default int getTimestampMicros() {
        return -1;
    }

    @Deprecated
    default String getColumnFamily() {
        return "values";
    }

    @Deprecated
    default Mutation buildSetMutation(final String columnQualifier, final Object value) {
        return buildSetMutation(getColumnFamily(), columnQualifier, value);
    }

    @Deprecated
    default Mutation buildSetMutation(final String columnQualifier, final ByteString byteString) {
        return buildSetMutation(getColumnFamily(), columnQualifier, byteString);
    }

    default Mutation buildSetMutation(
        final String columnFamily, final String columnQualifier, final Object value) {
        return buildSetMutation(
            columnFamily, columnQualifier, ByteString.copyFromUtf8(String.valueOf(value)));
    }

    default Mutation buildSetMutation(
        final String columnFamily, final String columnQualifier, final ByteString byteString) {
        return Mutation.newBuilder()
            .setSetCell(
                Mutation.SetCell.newBuilder()
                    .setFamilyName(columnFamily)
                    .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
                    .setValue(byteString)
                    .setTimestampMicros(getTimestampMicros()))
            .build();
    }
}

