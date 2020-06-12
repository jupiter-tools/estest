package com.jupitertools.estest.junit5;

import java.util.Date;

import com.jupitertools.estest.documents.Bar;
import com.jupitertools.estest.documents.Foo;
import com.jupitertools.estest.annotation.ElasticDataSet;
import com.jupitertools.springtestelasticsearch.ElasticsearchTestContainer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.GetQuery;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Created on 01.12.2018.
 *
 * @author Korovin Anatoliy
 */
@SpringBootTest
@ExtendWith(SpringExtension.class)
@ExtendWith(ElasticDataSetExtension.class)
@ElasticsearchTestContainer
class ElasticDataSetImportIT {

	private static final String FIRST_ID = "77f8f35b-93cf-40a1-9530-c13f2206cd76";
	private static final String SECOND_ID = "82f8f35b-93cf-40a1-9530-c13f2206cd77";

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;

	/**
	 * Testing import to the current database from a dataset defined in the {@link ElasticDataSet} annotation
	 */
	@Test
	@ElasticDataSet(value = "/dataset/extension/multidocument_dataset.json", cleanBefore = true, cleanAfter = true)
	void testImportByDataSetAnnotation() throws Exception {
		// Act
		Foo fooDoc = findById(FIRST_ID, Foo.class);
		Bar simpleDoc = findById(SECOND_ID, Bar.class);

		// Assert
		Assertions.assertThat(fooDoc)
		          .isNotNull()
		          .extracting(Foo::getId, Foo::getCounter, Foo::getTime)
		          .containsOnly(FIRST_ID, 1, new Date(1516527720000L));

		Assertions.assertThat(simpleDoc)
		          .isNotNull()
		          .extracting(Bar::getId, Bar::getData)
		          .containsOnly(SECOND_ID, "BB");
	}

	@Test
	@ElasticDataSet(value = "/dataset/extension/multidocument_dataset.json",
	                readOnly = true,
	                cleanBefore = true,
	                cleanAfter = true)
	void readOnly() {
		Foo fooDoc = findById(FIRST_ID, Foo.class);
		Assertions.assertThat(fooDoc).isNotNull();
	}

	@Test
	void withoutAnnotation() {
		//nothing to do
	}

	private <T> T findById(String id, Class<T> type) {
		GetQuery getQuery = GetQuery.getById(id);
		return elasticsearchTemplate.queryForObject(getQuery, type);
	}
}
