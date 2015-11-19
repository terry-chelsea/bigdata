package org.apache.kylin.client.meta;

import java.util.List;

//cube description about dimensions and measures
public class CubeDescMeta {
	private CubeMeta cubeDesc;
	
	private List<CubeDimensionMeta> dimensions;
	private List<CubeMeasureMeta> measures;
	
	public CubeDescMeta(CubeMeta cubeDesc, List<CubeDimensionMeta> dimensions,
			List<CubeMeasureMeta> measures) {
		this.cubeDesc = cubeDesc;
		this.dimensions = dimensions;
		this.measures = measures;
	}
	
	public CubeMeta getCubeDesc() {
		return cubeDesc;
	}
	public void setCubeDesc(CubeMeta cubeDesc) {
		this.cubeDesc = cubeDesc;
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
		return "CubeDescMeta [cubeDesc=" + cubeDesc + ", dimensions="
				+ dimensions + ", measures=" + measures + "]";
	}
	
}
