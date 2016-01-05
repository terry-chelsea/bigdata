package org.apache.kylin.client.meta;

public class CubeMeasureMeta {
	private String name;
	private String expression;
	private String paramType;
	private String value;
    private String returnType;
    
	public CubeMeasureMeta(String name, String expression, String paramType, String value, String returnType) {
		this.name = name;
		this.expression = expression;
		this.paramType = paramType;
		this.value = value;
		this.returnType = returnType;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getParamType() {
		return paramType;
	}

	public void setParamType(String paramType) {
		this.paramType = paramType;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public String getReturnType() {
		return returnType;
	}

	public void setReturnType(String returnType) {
		this.returnType = returnType;
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String toAggerateExpression() {
		if("COUNT_DISTINCT".equalsIgnoreCase(expression)) {
			return String.format("COUNT(DISTINCT %s)", value);
		}
		StringBuilder sb = new StringBuilder(expression);
        sb.append("(").append(value).append(")");
        return sb.toString();
	}

	@Override
	public String toString() {
		return "CubeMeasureMeta [name=" + name + ", expression=" + expression
				+ ", parameter type=" + paramType + ", value=" + value + ", returnType=" + returnType + "]";
	}
	
	/*
	static enum Expression {
		SUM(0),
		MIN(1),
		MAX(2),
		COUNT(3),
		DISTINCT_COUNT(4);
		
		int id;
		private Expression(int id) {
			this.id = id;
		}
	}
	
	static enum Type {
		CONSTANT(0),
		COLUMN(1);
		
		int id;
		private Type(int id) {
			this.id = id;
		}
	}
	*/
}
