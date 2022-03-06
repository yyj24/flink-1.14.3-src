package com.yyj.flink.watermark;


import org.apache.flink.api.common.eventtime.*;

import java.time.Duration;

public class WatermarkStrategyTest implements WatermarkStrategy {
    @Override
    public WatermarkGenerator createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return null;
    }

    @Override
    public TimestampAssigner createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return null;
    }

    @Override
    public WatermarkStrategy withTimestampAssigner(TimestampAssignerSupplier timestampAssigner) {
        return null;
    }

    @Override
    public WatermarkStrategy withTimestampAssigner(SerializableTimestampAssigner timestampAssigner) {
        return null;
    }

    @Override
    public WatermarkStrategy withIdleness(Duration idleTimeout) {
        return null;
    }
}
