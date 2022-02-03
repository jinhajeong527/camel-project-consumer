package com.vtw.jjh.route;


import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;

import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.model.dataformat.CsvDataFormat;
import org.springframework.stereotype.Component;

import com.vtw.jjh.processor.DataHistoryProcessor;
import com.vtw.jjh.processor.ErrorDataProcessor;
import com.vtw.jjh.processor.ExceptionHandleProcessor;
import com.vtw.jjh.processor.ManualCommitProcessor;


@Component
public class aTypeReceiveRoute extends RouteBuilder {
	

	@Override
	public void configure() throws Exception {
						
		
		//Json을 List<Map>형으로 unmarshal 하기위해 필요한 설정
		JacksonDataFormat format = new JacksonDataFormat();
		format.useList();
		format.setUnmarshalType(Map.class);
		
		//csv 설정
		CsvDataFormat csv = new CsvDataFormat();
		csv.setDelimiter(";");//구분 기호 ; 사용함
		
	
		//수신완료 시 historyWriteRoute로 direct
		//category는 send임
		onCompletion()
			.setProperty("category", constant("send"))
			.process(DataHistoryProcessor.NAME)
			.to("direct:historyWriteRoute")
			.log("sent to historyWriteRoute from aTypeReceiveRoute");
		
		
		//에러 발생시에 해당 데이터 파일에 관한 정보 생성(토픽, 오프셋, 에러 발생 데이터)
		onException(Exception.class)
			.choice()
				.when(new Predicate() {
						@Override
						public boolean matches(Exchange exchange) {
							File file = new File("C:/pass/failedData/"+exchange.getIn().getHeader("kafka.OFFSET", String.class)+"_offset_timestamp.json");
							System.out.println(file);
							if(file.exists()) {
							//해당 오프셋 제이슨 파일이 존재할 경우 수동 커밋 시켜야해서 트루 리턴함
							System.out.println("파일 존재");
							return true;
							} else {
							return false;
							}//if~else end
					}})
				.process(ManualCommitProcessor.NAME)
				.otherwise()
				.log("pass.csv file needed")
				.process(ExceptionHandleProcessor.NAME)
			.end();//choice end
		
		
		//check 폴더 확인하고 있다가 매뉴얼로 파일 옮겨주면 JSON 파일 생성해서 failedData 폴더로 옮겨줌
		//1초 마다 파일 monitoring
		from("file:C:/pass/check?noop=true&delay=10000&idempotentKey=${file:name}-${file:modified}").log("check directory로 옴")
		.convertBodyTo(String.class)//content 읽을 수 있게 해줌
		.process(ErrorDataProcessor.NAME)
		.marshal().json()
		.to("file:C:/pass/failedData?"//디렉토리 이름
				+ "fileName=${exchangeProperty.offset}_offset_timestamp.json")
		.log("json file for error data sent to folder failedData");
				
		//메인 라우트
		//data-a-jh topic에서 가져온 json data db에 넣어주기
		from("kafka:data-a-jh?"
				+ "groupId=CONSUMER-A"
				+ "&allowManualCommit=true"
				+ "&autoCommitEnable=false"
				+ "&breakOnFirstError=true")
		.routeId("aTypeReceiver")
		.process(e->{
			//에러 발생시키기
			Random random = new Random();
			int a = random.nextInt(2);
			if(a == 1) {
				throw new Exception("a 고의로 에러 발생시킴");
			}//if end
			
		})
		.unmarshal(format)//언마샬하면 header, property에 지정해준 값 사라짐
		.process(e->{
			//loop 돌리는 횟수 동적 데이타로 만들기 위해 작성함
			List<LinkedHashMap<String, Object>> orderList = new ArrayList<LinkedHashMap<String, Object>>();
			orderList = (List<LinkedHashMap<String, Object>>)e.getIn().getBody();
			Integer size = orderList.size();
			e.setProperty("size", constant(size));
		})
		.loop(simple("${exchangeProperty.size}"))
			.process(e -> {
				//LOOP_INDEX는 Camel 자체 속성. 0부터 시작해서 loop이 실행될 때 마다 1씩 증가한다.
				int index = e.getProperty(Exchange.LOOP_INDEX, Integer.class);
				System.out.println("index: "+ index);
				
				List<LinkedHashMap<String, Object>> orderList = new ArrayList<LinkedHashMap<String, Object>>();
				orderList = (List<LinkedHashMap<String, Object>>)e.getIn().getBody();
				
				System.out.println("orderList: "+orderList.get(index));
				e.setProperty("id", orderList.get(index).get("id"));
				e.setProperty("name", orderList.get(index).get("name"));
				e.setProperty("price", orderList.get(index).get("price"));
			
		})
		.to("sql:INSERT INTO DM01.ORDERS(ID, NAME, PRICE) " 
					+ "VALUES(:#${exchangeProperty.id}, :#${exchangeProperty.name}, :#${exchangeProperty.price})")
		.log("DB에 데이타 넣어주는 aType receiver worked")
		.end()//to end  
		.end();//loop end
		
		
	}//configure end

}//aTypeReceiveRoute end