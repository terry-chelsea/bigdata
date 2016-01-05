package org.apache.kylin.client.meta;

import java.util.List;

//cube description about dimensions and measures
public class CubeDescMeta {
	private String cubeName;
	
	private List<CubeDimensionMeta> dimensions;
	private List<CubeMeasureMeta> measures;
	
	public CubeDescMeta(String cubeName, List<CubeDimensionMeta> dimensions,
			List<CubeMeasureMeta> measures) {
		this.cubeName = cubeName;
		this.dimensions = dimensions;
		this.measures = measures;
	}
	
	public String getCubeName() {
		return cubeName;
	}

	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
	}

	public List<CubeDimensionMeta> getDimensions() {
		return dimensions;
	}
	public void setDimensions(List<CubeDimensionMeta> dimensions) {
		this.dimensions = dimensions;
	}
	public List<CubeMeasureMeta> getMeasures() {
		return measures;
	}
	public void setMeasures(List<CubeMeasureMeta> measures) {
		this.measures = measures;
	}

	@Override
	public String toString() {
		return "CubeDescMeta [cubeName=" + cubeName + ", dimensions="
				+ dimensions + ", measures=" + measures + "]";
	}
	
}
