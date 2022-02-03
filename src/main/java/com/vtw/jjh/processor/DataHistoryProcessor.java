package com.vtw.jjh.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import com.vtw.jjh.vo.DataHistory;



@Component(DataHistoryProcessor.NAME)
public class DataHistoryProcessor implements Processor {//Processor는 전달된 Exchange를 사용하거나, 생성하거나, 변경하는 핵심기능을 담당하는 노드
	
	public static final String NAME = "DataHistoryProcessor";

	@Override
	public void process(Exchange exchange) throws Exception {
		
		//송신 이력 정보 VO 객체 생성
		DataHistory dataHistory = new DataHistory();
		
		//onCompletion() 후에 send 혹은 receive로 category Property 설정해 준 것 받아와서 DataHistory VO에 넣어 주고자 함
		String category = "";
		category = (String)exchange.getProperty("category");
		
		//카테고리가 송신인지 수신인지 확인할 것
		if(category.equals("send")) {
			dataHistory.setCategory("send");
		} else {
			dataHistory.setCategory("receive");
		}//if~else end
		
		dataHistory.setExchangeId(exchange.getExchangeId());
		dataHistory.setAgentId(System.getProperty("agentId"));//VM arguments에 설정해 준 agentId 값 얻어오기
		dataHistory.setRouteId(exchange.getFromRouteId());
		dataHistory.setReceptionTime(System.currentTimeMillis());
		
		//송신처리 중 에러발생 여부 체크
		if(exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class) == null) {
			dataHistory.setResult("S");//success
		} else {
			dataHistory.setResult("F");//fail
		}//if~else end 
		
		exchange.getIn().setBody(dataHistory);
		
	}//process end 

}//Processor end