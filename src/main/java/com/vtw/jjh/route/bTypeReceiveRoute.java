package com.vtw.jjh.route;


import java.io.File;
import java.util.Map;
import java.util.Random;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jacksonxml.JacksonXMLDataFormat;
import org.apache.camel.model.dataformat.CsvDataFormat;
import org.springframework.stereotype.Component;

import com.vtw.jjh.processor.DataHistoryProcessor;
import com.vtw.jjh.processor.ErrorDataProcessor;
import com.vtw.jjh.processor.ExceptionHandleProcessor;
import com.vtw.jjh.processor.ManualCommitProcessor;



@Component
public class bTypeReceiveRoute extends RouteBuilder{

	@Override
	public void configure() throws Exception {
		
		//XML을 LIST<Map>로 unmarshal 하기위한 설정
		JacksonXMLDataFormat format = new JacksonXMLDataFormat();
		format.useList();
		format.setUnmarshalType(Map.class);
		
		
		//csv configuration
		CsvDataFormat csv = new CsvDataFormat();
		csv.setTrim("true");//간격 없애줌
		csv.setDelimiter("@");//구분 기호 @ 사용함
		
		//수신완료 시 historyWriteRoute로 direct
		//category는 send임
		onCompletion()
			.setProperty("category", constant("send"))
			.process(DataHistoryProcessor.NAME)
			.to("direct:historyWriteRoute");
		
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
				.log("pass.csv file needed!")
				.process(ExceptionHandleProcessor.NAME)
			.end();//choice end
		
		
		//check 폴더 확인하고 있다가 매뉴얼로 파일 옮겨주면 JSON 파일 생성해서 failedData 폴더로 옮겨줌
		//10초 마다 파일 monitoring
		from("file:C:/pass/checkForB?noop=true&delay=10000&idempotentKey=${file:name}-${file:modified}").log("bbb check directory로 옴")
		.convertBodyTo(String.class)//content 읽을 수 있게 해줌
		.process(ErrorDataProcessor.NAME)
		.marshal().json()
		.to("file:C:/pass/failedData?"//디렉토리 이름
					+ "fileName=${exchangeProperty.offset}_offset_timestamp.json")
		.log("json file for error data sent to folder failedData");
		
		
		//bTypeRecieveRoute의 경우 두개의 에이전트 모두에서 데이터를 수신해야 하기 때문에 그룹 아이디를 다르게 설정해주어야 함
		if(System.getProperty("agentId").equals("RECEIVE-AGENT-1")) {
			from("kafka:data-b-jh?"
					+ "groupId="+System.getProperty("groupId")//CONSUMER-B-1
					+ "&allowManualCommit=true"
					+ "&autoCommitEnable=false"
					+ "&breakOnFirstError=true")
			.routeId("bTypeReceiver")
			.process(e->{
				
				Random random = new Random();
				int a = random.nextInt(2);
				System.out.println(a);
				if(a == 1) {
					throw new Exception("b-1 고의로 에러 발생시킴");
				}
			})
			.log("bTypeReceiveRoute에서 작동된 agent?"+(String)System.getProperty("agentId"))
			.unmarshal(format)
			.marshal(csv).log("csv worked ${body}")
			.to("file:C:/exam/csvFile?"//디렉토리 이름
					+ "fileName=bTypeData_MenuList.csv"//파일명
					+ "&fileExist=Append");
		} else if(System.getProperty("agentId").equals("RECEIVE-AGENT-2")) {
			from("kafka:data-b-jh?"
					+ "groupId="+System.getProperty("groupId")//CONSUMER-B-2
					+ "&allowManualCommit=true"
					+ "&autoCommitEnable=false"
					+ "&breakOnFirstError=true")
			.routeId("bTypeReceiver")
			.process(e->{
				
				Random random = new Random();
				int a = random.nextInt(2);
				System.out.println(a);
				if(a == 1) {
					throw new Exception("b-2 고의로 에러 발생시킴");
				}
				
			})
			.log("RECEIVE-AGENT-2 작동 됨")
			.unmarshal(format)
			.marshal(csv).log("csv worked ${body}")
			.to("file:C:/exam/csvFile?"//디렉토리 이름
					+ "fileName=bTypeData_MenuList.csv"//파일명
					+ "&fileExist=Append");	
		}//if~else if end
		
	}//configure end 
		
}//bTypeReceiveRoute end