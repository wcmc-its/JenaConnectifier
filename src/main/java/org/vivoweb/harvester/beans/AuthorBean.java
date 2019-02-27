package org.vivoweb.harvester.beans;

/**
 * @author szd2013
 * <p><b><i> This author bean class stores all information related to WCMC authors <p><b><i>
 */
public class AuthorBean {
	
	private int authorshipPk;
	private String cwid;
	private String authName;
	private String givenName;
	private String surname;
	private String initials;
	private Long scopusAuthorId;
	private int authorshipRank;
	
	
	public int getAuthorshipPk() {
		return authorshipPk;
	}
	public void setAuthorshipPk(int authorshipPk) {
		this.authorshipPk = authorshipPk;
	}
	public String getCwid() {
		return cwid;
	}
	public void setCwid(String cwid) {
		this.cwid = cwid;
	}
	public String getAuthName() {
		return authName;
	}
	public void setAuthName(String authName) {
		this.authName = authName;
	}
	public String getGivenName() {
		return givenName;
	}
	public void setGivenName(String givenName) {
		this.givenName = givenName;
	}
	public String getSurname() {
		return surname;
	}
	public void setSurname(String surname) {
		this.surname = surname;
	}
	public String getInitials() {
		return initials;
	}
	public void setInitials(String initials) {
		this.initials = initials;
	}
	public Long getScopusAuthorId() {
		return scopusAuthorId;
	}
	public void setScopusAuthorId(Long scopusAuthorId) {
		this.scopusAuthorId = scopusAuthorId;
	}
	public int getAuthorshipRank() {
		return authorshipRank;
	}
	public void setAuthorshipRank(int authorshipRank) {
		this.authorshipRank = authorshipRank;
	}
	
	@Override
	public String toString() {
		return this.cwid + " " + this.authName + " " + this.authorshipRank;
	}
	
	
}
