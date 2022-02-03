package com.vtw.jjh.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;


@Component
public class HistoryReceiveRoute extends RouteBuilder {

	@Override
	public void configure() throws Exception {
		
		//jh-data-history 토픽에서 수신받은 데이터 historyWriteRoute로 전달함
		from("kafka:jh-data-history")
		.routeId("historyReceiveRoute")
		.unmarshal().json()
		.setProperty("category", constant("receive"))//unmarshal후에 category Property 안나옴 category property 다시 set 해줌
		.to("direct:historyWriteRoute");
	}

}