package com.vtw.jjh.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import com.vtw.jjh.processor.VOtoMAPProcessor;


@Component
public class HistoryWriteRoute extends RouteBuilder {

	@Override
	public void configure() throws Exception {
		
		from("direct:historyWriteRoute")
		.routeId("historyWriteRoute")
			.choice()
				//aTypeReceiveRoute, bTypeReceiveRoute에서 처리한 내역을 바로 보낸 것(구분값:송신)
				.when(simple("${exchangeProperty.category} == 'send'"))
					.process(VOtoMAPProcessor.NAME)
					.log("send 에러 없이 넘어옴")
					.to("sql:INSERT INTO HISTORY_JH(CATEGORY, EXCHANGE_ID, ROUTE_ID, AGENT_ID, RECEPTION_TIME, RESULT) "
							+ "VALUES(:#category, :#exchangeId, :#routeId,:#agentId,:#receptionTime,:#result)")
				//historyReceiveRoute에서 수신받은 데이터가 넘어온 것(구분값:수신)
				//여기의 경우는 VO를 JSON한 것을 언마샬한 HashMap이어서 따로 VOtoMAPProcessor를 거칠 필요가 없음
				.when(simple("${exchangeProperty.category} == 'receive'"))
					.log("recieve 에러 없이 넘어옴")
					.to("sql:INSERT INTO HISTORY_JH(CATEGORY, EXCHANGE_ID, ROUTE_ID, AGENT_ID, RECEPTION_TIME, RESULT) "
							+ "VALUES(:#category, :#exchangeId, :#routeId,:#agentId,:#receptionTime,:#result)")
			.end()//choice end
			.log("inserted into HISTORY_JH TABLE");
		
	} 
}