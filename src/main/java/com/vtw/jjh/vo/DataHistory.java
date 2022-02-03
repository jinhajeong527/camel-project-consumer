package com.vtw.jjh.vo;


public class DataHistory {
	
	private String exchangeId;
	private String agentId;
	private String routeId;
	private long receptionTime;
	private String result;//S,F(성공, 실패)
	private String category;//send, receive
	
	public DataHistory() {
	}

	public String getExchangeId() {
		return exchangeId;
	}

	public void setExchangeId(String exchangeId) {
		this.exchangeId = exchangeId;
	}

	public String getAgentId() {
		return agentId;
	}

	public void setAgentId(String agentId) {
		this.agentId = agentId;
	}

	public String getRouteId() {
		return routeId;
	}

	public void setRouteId(String routeId) {
		this.routeId = routeId;
	}

	public long getReceptionTime() {
		return receptionTime;
	}

	public void setReceptionTime(long receptionTime) {
		this.receptionTime = receptionTime;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}
	
	
	
	
	
	
}