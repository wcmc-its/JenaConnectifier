package org.vivoweb.harvester.beans;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FacultyAppointments {
	
	@JsonProperty(value = "uid", required = true)
	private String uid;
	@JsonProperty(value = "weillCornellEduCWID", required = true)
	private String cwid;
	@JsonProperty(value = "weillCornellEduDepartment", required = true)
	private String departmentId;
	@JsonProperty(value = "weillCornellEduDepartmentLabel", required = true)
	private String department;
	@JsonProperty(value = "weillCornellEduStartDate", required = true)
	private String startDate;
	@JsonProperty(value = "weillCornellEduEndDate", required = true)
	private String endDate;
	private String modifyTimeStamp;
	
	private static Logger log = LoggerFactory.getLogger(FacultyAppointments.class);
	
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getCwid() {
		return cwid;
	}
	public void setCwid(String cwid) {
		this.cwid = cwid;
	}
	public String getDepartment() {
		return department;
	}
	public void setDepartment(String department) {
		if(department==null)
			this.department = "NULL";
		else
			this.department =  department;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		if(startDate==null)
			this.startDate = "NULL";
		else
			this.startDate = startDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		if(endDate==null)
			this.endDate = "NULL";
		else
			this.endDate = endDate;
	}
	public String getModifyTimeStamp() {
		return modifyTimeStamp;
	}
	public void setModifyTimeStamp(String modifyTimeStamp) {
		this.modifyTimeStamp = modifyTimeStamp;
	}
	
	
	
	public String getDepartmentId() {
		return departmentId;
	}
	public void setDepartmentId(String departmentId) {
		this.departmentId = departmentId;
	}
	@Override
	public String toString() {
		log.info(this.uid + " " + this.cwid + " " + this.department + " " + this.startDate + " " + this.endDate + " " + this.modifyTimeStamp);
		return this.uid + " " + this.cwid + " " + this.department + " " + this.startDate + " " + this.endDate + " " + this.modifyTimeStamp;
	}
	
}

