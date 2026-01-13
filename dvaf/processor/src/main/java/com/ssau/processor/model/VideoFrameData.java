package com.ssau.processor.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VideoFrameData {

    @JsonProperty("camId")
    private String camId;

    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;

    @JsonProperty("rows")
    private int rows;

    @JsonProperty("cols")
    private int cols;

    @JsonProperty("type")
    private int type;

    @JsonProperty("data")
    private String data;
}

