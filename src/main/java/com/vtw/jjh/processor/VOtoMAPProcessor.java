package com.vtw.jjh.processor;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import com.vtw.jjh.vo.DataHistory;



@Component(VOtoMAPProcessor.NAME)
public class VOtoMAPProcessor implements Processor {
	
	public static final String NAME = "VOtoMAPProcessor";

	@Override
	public void process(Exchange exchange) throws Exception {
		//aTypeReceiveRoute, bTypeReceiveRoute에서 처리한 이력 정보 바로 보낸 경우 DataHistory VO로 오기 때문에  DB에 넣기 전에 바꿔주어야 해 작성한 프로세서
		System.out.println("여기는 VOtoMAPProcessor");
		
		DataHistory dataHistory = (DataHistory)exchange.getIn().getBody();
		
		Map<String, Object> history = new HashMap<>();
		history.put("category", dataHistory.getCategory());
		history.put("exchangeId", dataHistory.getExchangeId());
		history.put("routeId", dataHistory.getRouteId());
		history.put("agentId", dataHistory.getAgentId());
		history.put("receptionTime", dataHistory.getReceptionTime());
		history.put("result", dataHistory.getResult());
		
		exchange.getIn().setBody(history);
		
		
	}//process end 

}//Processor end