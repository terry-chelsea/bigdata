package org.apache.kylin.client.meta;

import java.util.List;

public class CreateMeasureMeta {
	private String name;
	private MeasureType type;
	private String value;
	private String dataType;
	private List<String> expressions;
	
	public static enum MeasureType {
		constant, column;
	}


	public CreateMeasureMeta(String name, MeasureType type, String value,
			String dataType, List<String> expressions) {
		super();
		this.name = name;
		this.type = type;
		this.value = value;
		this.dataType = dataType;
		this.expressions = expressions;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public MeasureType getType() {
		return type;
	}

	public void setType(MeasureType type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public List<String> getExpressions() {
		return expressions;
	}

	public void setExpressions(List<String> expressions) {
		this.expressions = expressions;
	}
	
	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	@Override
	public String toString() {
		return "CreateMeasureMeta [name=" + name + ", type=" + type
				+ ", value=" + value + ", dataType=" + dataType
				+ ", expressions=" + expressions + "]";
	}
}
