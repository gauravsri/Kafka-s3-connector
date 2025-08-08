package com.company.kafkaconnector.service;

import com.company.kafkaconnector.model.TopicConfig;
import com.company.kafkaconnector.utils.ColumnarBatchUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.expressions.Literal;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class DeltaKernelWriterService {

    private final Engine engine;
    private final ObjectMapper objectMapper;

    public DeltaKernelWriterService(Engine engine, ObjectMapper objectMapper) {
        this.engine = engine;
        this.objectMapper = objectMapper;
    }

    public void write(
            TopicConfig topicConfig,
            List<Map<String, Object>> batch,
            StructType schema) throws IOException {

        String tablePath = String.format("s3a://%s/%s",
                topicConfig.getDestination().getBucket(),
                topicConfig.getDestination().getPath());

        Table table = Table.forPath(engine, tablePath);
        TransactionBuilder txBuilder = table.createTransactionBuilder(
                engine,
                "kafka-connector",
                Operation.WRITE
        );

        Transaction txn = txBuilder
                .withSchema(engine, schema)
                .withPartitionColumns(engine, topicConfig.getDestination().getPartitionColumns())
                .build(engine);

        Row txnState = txn.getTransactionState(engine);

        ColumnVector jsonVector = ColumnarBatchUtils.createJsonStringVector(batch, objectMapper);
        ColumnarBatch columnarBatch = engine.getJsonHandler().parseJson(jsonVector, schema, Optional.empty());
        FilteredColumnarBatch filteredColumnarBatch = new FilteredColumnarBatch(columnarBatch, Optional.empty());

        CloseableIterable<FilteredColumnarBatch> data = CloseableIterable.inMemoryIterable(
            new CloseableIterator<FilteredColumnarBatch>() {
                private boolean hasNext = true;
                @Override
                public boolean hasNext() {
                    return hasNext;
                }
                @Override
                public FilteredColumnarBatch next() {
                    hasNext = false;
                    return filteredColumnarBatch;
                }
                @Override
                public void close() {}
            }
        );

        DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, Collections.emptyMap());

        CloseableIterator<DataFileStatus> writtenFiles = engine.getParquetHandler().writeParquetFiles(
            writeContext.getTargetDirectory(),
            data.iterator(),
            writeContext.getStatisticsColumns()
        );

        CloseableIterable<Row> commitActions = CloseableIterable.inMemoryIterable(
            Transaction.generateAppendActions(
                engine,
                txnState,
                writtenFiles,
                writeContext
            )
        );

        txn.commit(engine, commitActions);
    }
}
