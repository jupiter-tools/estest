package com.jupitertools.estest.junit5;

import java.util.UUID;

import com.jupitertools.estest.documents.Bar;
import com.jupitertools.estest.annotation.ElasticDataSet;
import com.jupitertools.estest.internal.ElasticsearchDataTools;
import com.jupitertools.springtestelasticsearch.ElasticsearchTestContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
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
@ExtendWith(SpringExtension.class)
@ExtendWith({// The order of these extensions is important
             ElasticDataSetCleanBeforeIT.PopulateDataBeforeTestExtension.class,
             ElasticDataSetExtension.class})
@ElasticsearchTestContainer
class ElasticDataSetCleanBeforeIT {

	private static final UUID TEST_ENTITY_ID = UUID.fromString("82f8f35b-93cf-40a1-9530-c13f2206cd76");

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;

	@Test
	@ElasticDataSet(/* The second step: */ cleanBefore = true)
	void cleanBefore() {
		// The third step:
		GetQuery getQuery = GetQuery.getById(TEST_ENTITY_ID.toString());
		Bar simpleDoc = elasticsearchTemplate.queryForObject(getQuery, Bar.class);
		assertThat(simpleDoc).isNull();
	}

	public static class PopulateDataBeforeTestExtension implements Extension, BeforeAllCallback {

		@Override
		public void beforeAll(ExtensionContext context) throws Exception {
			// The first step:
			ElasticsearchTemplate template = SpringExtension.getApplicationContext(context)
			                                                .getBean(ElasticsearchTemplate.class);
			new ElasticsearchDataTools(template).importFrom("/dataset/extension/simple_dataset.json");
		}
	}
}
