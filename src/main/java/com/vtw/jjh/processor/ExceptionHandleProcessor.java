package com.vtw.jjh.processor;



import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component(ExceptionHandleProcessor.NAME)
public class ExceptionHandleProcessor implements Processor {
	//pass.csv 파일 생성위해 작성한 프로세서
	public static final String NAME = "ExceptionHandleProcessor";

	@Override
	public void process(Exchange exchange) throws Exception {
		
		System.out.println("여기는 ExceptionHandleProcessor");
		
		//csv 언마샬 후에 body가 비어버려서 실패한 데이터 내용이 body에 남아 있을 때 header에 셋해줌
		String failedData = (String)exchange.getIn().getBody();
		exchange.getIn().setHeader("failedData", failedData);
		
		
		//Map<String, Object> pass = new LinkedHashMap<>();
		
		//에러 떨어진 데이터의 토픽과 오프셋 얻어오기
		String topic = (String)exchange.getIn().getHeader("kafka.TOPIC");
		Long offset = (Long)exchange.getIn().getHeader("kafka.OFFSET");//카프카 오프셋은 Long형임
		Long timestamp = (Long)exchange.getIn().getHeader("kafka.TIMESTAMP");
		
		//pass.put("topic", topic);
		//pass.put("offset", offset);
		//exchange.getIn().setBody(pass);
		
		StringBuilder csv = new StringBuilder(); 
		csv.append(topic);
		csv.append(";").append(offset);
		csv.append(";").append(timestamp);
		csv.append(";").append(failedData);
		
		exchange.getIn().setBody(csv.toString()); 
		
		exchange.getContext().createProducerTemplate().sendBody("file:C:/pass?fileName=pass.csv&noop=true",exchange.getIn().getBody());
		//System.out.println("토픽"+topic);
		//System.out.println("오프셋"+offset);데이타 남아 있음 yet
		System.out.println("pass.csv 파일 생성돼서 pass폴더로 보내짐");
		
		//exchange.setProperty("topic", topic);
		//exchange.setProperty("offset", offset);
		//exchange.setProperty("failedData", failedData);
		//exchange.setProperty("timeStamp", timestamp);
		
		
	}//process end 

}//Processor end