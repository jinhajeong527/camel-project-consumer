package com.vtw.jjh.processor;


import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;


@Component(ErrorDataProcessor.NAME)
public class ErrorDataProcessor implements Processor {
	//오류 데이타 json 파일생성에 필요한 정보 묶어주기 위해 작성한 Processor
	public static final String NAME = "ErrorDataProcessor";

	@Override
	public void process(Exchange exchange) throws Exception {
		
		System.out.println("여기는 ErrorDataProcessor");
		
		String failedDataInfoStr = (String)exchange.getIn().getBody();
		
		int count = 0;
		//;로 구분된 csv 파일의 ; 개수 세보기
        for(int i = 0; i < failedDataInfoStr.length(); i++) {
            if (failedDataInfoStr.charAt(i) == ';') count++;
        }
        
        System.out.println("; 의 개수는? "+count);//3
        //; 위치 담을 인트 배열
        int[] location = new int[count];
        //필요 정보 담을 스트링 배열
        String[] info = new String[count+1];
        
        for(int i = 0; i < count ; i++) {
        	location[i] = failedDataInfoStr.indexOf(";");
        	info[i] = failedDataInfoStr.substring(0, location[i]);
        	failedDataInfoStr = failedDataInfoStr.replace(info[i]+";","");
        	System.out.println("failedDataInfoStr "+failedDataInfoStr);
        }
        
        info[count] = failedDataInfoStr;
		
		
		Map<String, Object> failedDataInfo = new LinkedHashMap<>();
		
		
		failedDataInfo.put("topic", info[0]);
		failedDataInfo.put("offset", info[1]);
		failedDataInfo.put("timestamp", info[2]);
		failedDataInfo.put("failedData", info[count]);
		
		exchange.getIn().setBody(failedDataInfo);
		//제목에 offset 붙이기 위함.
		exchange.setProperty("offset", info[1]);
	
		
	}//process end 

}//Processor end