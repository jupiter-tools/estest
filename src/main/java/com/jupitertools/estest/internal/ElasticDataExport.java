package com.jupitertools.estest.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jupitertools.datasetroll.DataSet;
import com.jupitertools.datasetroll.exportdata.DataSetExport;
import com.jupitertools.estest.internal.scanner.ElasticDocumentScanner;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;

/**
 * Created on 18/11/2019
 * <p>
 * Exports all documents from ElasticSearch database to the {@link DataSet} instance.
 *
 * @author Korovin Anatoliy
 */
public class ElasticDataExport implements DataSetExport {

	private final ElasticsearchTemplate elasticsearchTemplate;
	private final ObjectMapper objectMapper;
	private final Logger log;

	private final static NativeSearchQueryBuilder MATCH_ALL_QUERY =
			new NativeSearchQueryBuilder()
					.withQuery(QueryBuilders.matchAllQuery());


	public ElasticDataExport(ElasticsearchTemplate elasticsearchTemplate) {
		this.elasticsearchTemplate = elasticsearchTemplate;
		this.objectMapper = new ObjectMapper();
		this.log = LoggerFactory.getLogger(this.getClass());
	}

	@Override
	public DataSet export() {

		Map<String, Class<?>> documents = new ElasticDocumentScanner("").scan();
		log.debug("Scanned elasticsearch documents: {}", documents);
		Map<String, List<Map<String, Object>>> dataSet = new LinkedHashMap<>();
		List<Class<?>> sortedDocuments = documents.values()
		                                          .stream()
		                                          .sorted(Comparator.comparing(Class::getName))
		                                          .collect(Collectors.toList());

		for (Class<?> documentClass : sortedDocuments) {
			if (isNothingToExport(documentClass)) {
				continue;
			}
			dataSet.put(documentClass.getName(), getListOfRecordsFromElastic(documentClass));
		}
		return () -> dataSet;
	}


	private boolean isNothingToExport(Class<?> documentClass) {
		return !elasticsearchTemplate.indexExists(documentClass) ||
		       getCount(documentClass) == 0;
	}

	private long getCount(Class<?> clazz) {
		return elasticsearchTemplate.count(MATCH_ALL_QUERY.build(), clazz);
	}

	private List<Map<String, Object>> getListOfRecordsFromElastic(Class<?> documentClass) {

		List<Map<String, Object>> result = new ArrayList<>();
		elasticsearchTemplate.stream(MATCH_ALL_QUERY.build(), documentClass)
		                     .forEachRemaining(record -> result.add(objectMapper.convertValue(record, Map.class)));

		return result;
	}
}
