package com.vtw.jjh.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.springframework.stereotype.Component;

@Component(ManualCommitProcessor.NAME)
public class ManualCommitProcessor implements Processor {

	public static final String NAME = "ManualCommitProcessor";

	@Override
	public void process(Exchange exchange) {

		KafkaManualCommit manual = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);

		if (manual == null) {
			manual = exchange.getProperty(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
		}

		manual.commitSync();
	}
}
