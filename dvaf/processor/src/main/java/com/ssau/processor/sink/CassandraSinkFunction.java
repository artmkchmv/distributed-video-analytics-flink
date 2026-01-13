package com.ssau.processor.sink;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import lombok.extern.slf4j.Slf4j;

import com.ssau.processor.model.ProcessingResult;
import com.ssau.processor.service.CassandraService;

@Slf4j
public class CassandraSinkFunction extends RichSinkFunction<ProcessingResult> {
    
    private transient CassandraService cassandraService;
    private final Properties cassandraProps;
    
    public CassandraSinkFunction(Properties cassandraProps) {
        this.cassandraProps = cassandraProps;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        cassandraService = new CassandraService(cassandraProps);
        log.info("Cassandra sink function opened");
    }
    
    @Override
    public void invoke(ProcessingResult value, Context context) throws Exception {
        if (cassandraService != null && value != null) {
            cassandraService.insertResult(
                value.getCameraId(),
                value.getFrameTimestamp(),
                value.getDetectionType(),
                value.getDetectionCount(),
                value.getFrameRows(),
                value.getFrameCols(),
                value.getImagePath(),
                value.getMetadata()
            );
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        if (cassandraService != null) {
            cassandraService.close();
        }
    }
}

