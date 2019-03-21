/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.pmml.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.hamcrest.core.StringContains;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.cloud.stream.test.matcher.MessageQueueMatcher.receivesPayloadThat;

/**
 * Integration Tests for PmmlProcessor.
 *
 * @author Eric Bottard
 * @author Gary Russell
 * @author Artem Bilan
 * @author Christian Tzolov
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = "pmml.modelLocation=classpath:pmml/iris-flower-classification-naive-bayes-1.pmml.xml")
@DirtiesContext
public abstract class PmmlProcessorIntegrationTests {

	@Autowired
	protected Processor channels;

	@Autowired
	protected MessageCollector messageCollector;

	@TestPropertySource(properties = {
			"pmml.inputs:Sepal.Length = payload.sepalLength," +
					"Sepal.Width = payload.sepalWidth," +
					"Petal.Length = payload.petalLength," +
					"Petal.Width = payload.petalWidth",
			"pmml.outputs: Predicted_Species=payload.predictedSpecies" })
	public static class SimpleMappingJsonInputTests extends PmmlProcessorIntegrationTests {

		@Test
		public void testEvaluation() {
			String payload = "{ \"sepalLength\": 6.4, \"sepalWidth\": 3.2, \"petalLength\":4.5, \"petalWidth\":1.5 }";
			channels.input().send(MessageBuilder
					.withPayload(payload)
					.setHeader(MessageHeaders.CONTENT_TYPE, "application/json").build());

			assertThat(messageCollector.forChannel(channels.output()),
					receivesPayloadThat(StringContains.containsString("\"predictedSpecies\":\"versicolor\"")));
		}

		@Test
		public void testEvaluationOriginalContentType() {
			String payload = "{ \"sepalLength\": 6.4, \"sepalWidth\": 3.2, \"petalLength\":4.5, \"petalWidth\":1.5 }";
			channels.input().send(MessageBuilder
					.withPayload(payload)
					.setHeader(MessageHeaders.CONTENT_TYPE, "text/plain")
					.setHeader("originalContentType", "application/json;charset=UTF-8")
					.build());

			assertThat(messageCollector.forChannel(channels.output()),
					receivesPayloadThat(StringContains.containsString("\"predictedSpecies\":\"versicolor\"")));
		}

		@Test
		public void testEvaluationDefaultContentType() {
			String payload = "{ \"sepalLength\": 6.4, \"sepalWidth\": 3.2, \"petalLength\":4.5, \"petalWidth\":1.5 }";
			channels.input().send(MessageBuilder
					.withPayload(payload)
					.build());

			assertThat(messageCollector.forChannel(channels.output()),
					receivesPayloadThat(StringContains.containsString("\"predictedSpecies\":\"versicolor\"")));
		}

	}


	@TestPropertySource(properties = {
			"pmml.inputs:Sepal.Length = payload.sepalLength," +
					"Sepal.Width = payload.sepalWidth," +
					"Petal.Length = payload.petalLength," +
					"Petal.Width = payload.petalWidth",
			"pmml.outputs: Predicted_Species=payload.predictedSpecies" })
	public static class SimpleMappingTests extends PmmlProcessorIntegrationTests {

		@Test
		public void testEvaluation() {
			Map<String, String> payload = new HashMap<>();
			payload.put("sepalLength", "6.4");
			payload.put("sepalWidth", "3.2");
			payload.put("petalLength", "4.5");
			payload.put("petalWidth", "1.5");
			channels.input().send(MessageBuilder.withPayload(payload).build());

			assertThat(messageCollector.forChannel(channels.output()),
					receivesPayloadThat(StringContains.containsString("\"predictedSpecies\":\"versicolor\"")));
		}

	}

	public static class NoMappingTests extends PmmlProcessorIntegrationTests {

		@Test
		public void testEvaluation() {
			Map<String, Object> payload = new HashMap<>();
			Map<String, String> sepal = new HashMap<>();
			Map<String, String> petal = new HashMap<>();
			payload.put("Sepal", sepal);
			payload.put("Petal", petal);
			sepal.put("Length", "6.4");
			sepal.put("Width", "3.2");
			petal.put("Length", "4.5");
			petal.put("Width", "1.5");
			channels.input().send(MessageBuilder.withPayload(payload).build());

			BlockingQueue<Message<?>> output = messageCollector.forChannel(channels.output());

			assertThat(output,
					receivesPayloadThat(StringContains.containsString("\"Predicted_Species\":\"versicolor\"")));
		}

	}

	@SpringBootApplication
	public static class PmmlProcessorApplication {

	}

}
