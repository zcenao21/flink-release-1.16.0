package org.apache.flink.streaming.examples.windowing.will;

/**
 * WaterSensor.
 */
public class WaterSensor {
	public String id;
	public long ts; // 时间
	public int vc; // 水位

	public WaterSensor(String id, long ts, int vc) {
		this.id = id;
		this.ts = ts;
		this.vc = vc;
	}

	public String toString() {
		return "id:" + id + ", ts:" + ts + ", vc:" + vc;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public int getVc() {
		return vc;
	}

	public void setVc(int vc) {
		this.vc = vc;
	}
}
