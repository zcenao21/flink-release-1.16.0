package org.apache.flink.streaming.examples.windowing.will;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.examples.windowing.will.WaterSensor;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
	@Override
	public WaterSensor map(String line) throws Exception {
		String[] parts = line.split(",");
		return new WaterSensor(parts[0], Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
	}
}
