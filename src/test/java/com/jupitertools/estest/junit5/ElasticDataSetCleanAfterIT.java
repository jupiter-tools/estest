package com.jupitertools.estest.junit5;

import java.util.UUID;

import com.jupitertools.estest.documents.Bar;
import com.jupitertools.estest.annotation.ElasticDataSet;
import com.jupitertools.estest.internal.ElasticsearchDataTools;
import com.jupitertools.springtestelasticsearch.ElasticsearchTestContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.GetQuery;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created on 01.12.2018.
 *
 * @author Korovin Anatoliy
 */
@SpringBootTest
@ExtendWith({// The order of extensions is an important:
             ElasticDataSetExtension.class,
             ElasticDataSetCleanAfterIT.TestCleanAfterExtension.class})
@ElasticsearchTestContainer
class ElasticDataSetCleanAfterIT {

	private static final UUID TEST_ENTITY_ID = UUID.fromString("82f8f35b-93cf-40a1-9530-c13f2206cd76");

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;

	@BeforeEach
	void setUp() {
		// The first step:
		new ElasticsearchDataTools(elasticsearchTemplate).importFrom("/dataset/extension/simple_dataset.json");
	}

	@Test
	@ElasticDataSet(/* The third step: */ cleanAfter = true)
	void cleanAfter() {
		// The second step:
		GetQuery getQuery = GetQuery.getById(TEST_ENTITY_ID.toString());
		Bar simpleDoc = elasticsearchTemplate.queryForObject(getQuery, Bar.class);
		assertThat(simpleDoc).isNotNull();
	}

	public static class TestCleanAfterExtension implements Extension, AfterAllCallback {

		@Override
		public void afterAll(ExtensionContext context) throws Exception {
			ElasticsearchTemplate template = SpringExtension.getApplicationContext(context)
			                                                .getBean(ElasticsearchTemplate.class);
			// Fourth step:
			GetQuery getQuery = GetQuery.getById(TEST_ENTITY_ID.toString());
			Bar simpleDoc = template.queryForObject(getQuery, Bar.class);
			assertThat(simpleDoc).isNull();
		}
	}
}
