package org.apache.kylin.client.meta;

//cube base infomation
public class CubeMeta {
	private String cubeName;
	private boolean enable;
	private long createTime;
	private long retentionRange;
	private String projectName;
	private long cubeSizeKb;
	private long rangeStart;
	private long rangeEnd;
	
	private ProjectMeta project;
	
	public CubeMeta(String cubeName, boolean enable,
			long createTime, long retentionRange, String projectName,
			long cubeSizeKb, long rangeStart, long rangeEnd) {
		super();
		this.cubeName = cubeName;
		this.enable = enable;
		this.createTime = createTime;
		this.retentionRange = retentionRange;
		this.projectName = projectName;
		this.cubeSizeKb = cubeSizeKb;
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
	}
	public String getCubeName() {
		return cubeName;
	}
	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
	}

	public boolean isEnable() {
		return enable;
	}
	public void setEnable(boolean enable) {
		this.enable = enable;
	}
	public long getCreateTime() {
		return createTime;
	}
	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}
	public long getRetentionRange() {
		return retentionRange;
	}
	public void setRetentionRange(int retentionRange) {
		this.retentionRange = retentionRange;
	}
	public ProjectMeta getProject() {
		return project;
	}
	public void setProjectName(ProjectMeta project) {
		this.project = project;
	}
	public long getCubeSizeKb() {
		return cubeSizeKb;
	}
	public void setCubeSizeKb(long cubeSizeKb) {
		this.cubeSizeKb = cubeSizeKb;
	}
	public long getRangeStart() {
		return rangeStart;
	}
	public void setRangeStart(long rangeStart) {
		this.rangeStart = rangeStart;
	}
	public long getRangeEnd() {
		return rangeEnd;
	}
	public void setRangeEnd(long rangeEnd) {
		this.rangeEnd = rangeEnd;
	}
	@Override
	public String toString() {
		return "CubeMeta [cubeName=" + cubeName + ", enable=" + enable + ", createTime="
				+ createTime + ", retentionRange=" + retentionRange
				+ ", project=" + project + ", cubeSizeKb=" + cubeSizeKb
				+ ", rangeStart=" + rangeStart + ", rangeEnd=" + rangeEnd + "]";
	}
	
}
