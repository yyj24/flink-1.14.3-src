package com.yyj.flink.fn;

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MyProcessFunction extends ProcessFunction<String, String> {

    final OutputTag<String> outputTag = new OutputTag<>("side-output");

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        out.collect(value);
        ctx.output(outputTag, value);

        TimerService timerService = ctx.timerService();
        timerService.currentWatermark();
        timerService.currentProcessingTime();
        timerService.deleteEventTimeTimer(0l);
        timerService.deleteProcessingTimeTimer(0l);
        timerService.registerEventTimeTimer(0l);
        timerService.registerProcessingTimeTimer(0l);

        ctx.timestamp();
    }
}
