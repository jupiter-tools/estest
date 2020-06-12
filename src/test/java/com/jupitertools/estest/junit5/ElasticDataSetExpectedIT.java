package com.jupitertools.estest.junit5;

import java.util.Date;

import com.jupitertools.estest.documents.Bar;
import com.jupitertools.estest.documents.Foo;
import com.jupitertools.estest.annotation.ElasticDataSet;
import com.jupitertools.estest.annotation.EnableElasticDataSet;
import com.jupitertools.estest.annotation.ExpectedElasticDataSet;
import com.jupitertools.springtestelasticsearch.ElasticsearchTestContainer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.GetQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

/**
 * Created on 12.12.2018.
 *
 * @author Korovin Anatoliy
 */
@Disabled("TODO: find a way to test fails throws in extensions callbacks")
@SpringBootTest
@EnableElasticDataSet
@ElasticsearchTestContainer
class ElasticDataSetExpectedIT {

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;

	@Test
	@ElasticDataSet(cleanAfter = true, cleanBefore = true)
	@ExpectedElasticDataSet("/dataset/extension/ignored/expected_dataset.json")
	void testExpected() {
		Bar bar1 = new Bar("111100001", "data-1");
		Bar bar2 = new Bar("111100002", "data-2");
		save(bar1, Bar.class);
		save(bar2, Bar.class);
	}

	@Test
	@ElasticDataSet(cleanAfter = true, cleanBefore = true)
	@ExpectedElasticDataSet("/dataset/extension/ignored/expected_dataset.json")
	void testExpectWithNotSame() {
		Bar bar1 = new Bar("111100001", "data-1");
		Bar bar2 = new Bar("111100002", "data-3");
		save(bar1, Bar.class);
		save(bar2, Bar.class);
	}

	@Test
	@ElasticDataSet(cleanAfter = true, cleanBefore = true)
	@ExpectedElasticDataSet("/dataset/extension/ignored/expected_dataset.json")
	void testExpectWhenDbIsEmpty() {
		// nope
	}


	@Test
	@ElasticDataSet(cleanAfter = true, cleanBefore = true)
	@ExpectedElasticDataSet("/dataset/extension/ignored/expected_dataset_multiple.json")
	void testExpectWithMultipleCollections() {
		Bar bar1 = new Bar("111100001", "data-1");
		Bar bar2 = new Bar("111100002", "data-2");
		save(bar1, Bar.class);
		save(bar2, Bar.class);
		Foo foo1 = new Foo("F1", new Date(), 1);
		Foo foo2 = new Foo("F2", new Date(), 2);
		Foo foo3 = new Foo("F3", new Date(), 3);
		save(foo1, Foo.class);
		save(foo2, Foo.class);
		save(foo3, Foo.class);
	}


	@Test
	@ElasticDataSet(cleanAfter = true, cleanBefore = true)
	@ExpectedElasticDataSet("/dataset/extension/ignored/expected_dataset_multiple.json")
	void testExpectWithMultipleCollectionsCombination() {
		Bar bar1 = new Bar("111100001", "data-1");
		Bar bar2 = new Bar("111100002", "data-2");
		save(bar1, Bar.class);
		save(bar2, Bar.class);
		Foo foo1 = new Foo("F1", new Date(12345001), 1);
		Foo foo2 = new Foo("F2", new Date(12345002), 3);
		Foo foo3 = new Foo("F3", new Date(12345003), 3);
		save(foo1, Foo.class);
		save(foo2, Foo.class);
		save(foo3, Foo.class);
	}

	@Test
	@ElasticDataSet(cleanAfter = true, cleanBefore = true)
	@ExpectedElasticDataSet("/dataset/extension/ignored/expected_dataset_double_matching.json")
	void testExpectWithMultipleCollectionsDoubleMatching() {
		Bar bar1 = new Bar("111100001", "data-1");
		Bar bar2 = new Bar("111100002", "data-2");
		Bar bar3 = new Bar("111100003", "data-2");
		save(bar1, Bar.class);
		save(bar2, Bar.class);
		save(bar3, Bar.class);
		Foo foo1 = new Foo("F1", new Date(), 1);
		Foo foo2 = new Foo("F2", new Date(), 3);
		Foo foo3 = new Foo("F3", new Date(), 3);
		Foo foo4 = new Foo("F4", new Date(), 4);
		Foo foo5 = new Foo("F5", new Date(), 3);
		save(foo1, Foo.class);
		save(foo2, Foo.class);
		save(foo3, Foo.class);
		save(foo4, Foo.class);
		save(foo5, Foo.class);
	}

	@Test
	@ElasticDataSet(cleanAfter = true, cleanBefore = true)
	@ExpectedElasticDataSet("/dataset/extension/ignored/expected_dataset_multiple.json")
	void testExpectNotExistsCollection() {
		Bar bar1 = new Bar("111100001", "data-1");
		Bar bar2 = new Bar("111100002", "data-2");
		save(bar1, Bar.class);
		save(bar2, Bar.class);
	}

	@Test
	@ElasticDataSet(value = "/dataset/extension/ignored/multidocument_dataset.json",
	                readOnly = true,  // ASSERT THIS
	                cleanBefore = true,
	                cleanAfter = true)
	void readOnlyFail() {
		Foo fooDoc = findById("77f3ed00b1375a48e618300a", Foo.class);
		fooDoc.setCounter(51187);
		save(fooDoc, Foo.class);
	}

	private <T> void save(T entity, Class<T> type) {
		elasticsearchTemplate.index(new IndexQueryBuilder().withObject(entity).build());
		elasticsearchTemplate.refresh(type);
	}

	private <T> T findById(String id, Class<T> type) {
		GetQuery getQuery = GetQuery.getById(id);
		return elasticsearchTemplate.queryForObject(getQuery, type);
	}
}
