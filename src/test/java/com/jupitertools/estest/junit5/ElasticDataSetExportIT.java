package com.jupitertools.estest.junit5;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import com.jupitertools.estest.annotation.ElasticDataSet;
import com.jupitertools.estest.annotation.ExportElasticDataSet;
import com.jupitertools.estest.internal.ElasticsearchDataTools;
import com.jupitertools.springtestelasticsearch.ElasticsearchTestContainer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created on 03.12.2018.
 *
 * @author Korovin Anatoliy
 */
@SpringBootTest
@ExtendWith(SpringExtension.class)
@ExtendWith(ElasticDataSetExportIT.ExportTestExtension.class)
@ExtendWith(ElasticDataSetExtension.class)
@ElasticsearchTestContainer
class ElasticDataSetExportIT {

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;

	private static final String INPUT_DATA_SET_FILE = "/dataset/extension/exported_dataset.json";
	private static final String OUTPUT_FILE_NAME = "target/dataset/export.json";

	@Test
	@ElasticDataSet(cleanBefore = true)
	@ExportElasticDataSet(outputFile = OUTPUT_FILE_NAME)
	void exportDataSet() {
		// TODO: try to test it without ElasticsearchDataTools,
		//  the using of an external tool will be more clearly solution
		//  to be sure in stability of this implementation
		new ElasticsearchDataTools(elasticsearchTemplate).importFrom(INPUT_DATA_SET_FILE);
	}

	static class ExportTestExtension implements Extension, AfterEachCallback, BeforeEachCallback {

		@Override
		public void afterEach(ExtensionContext context) throws Exception {
			try (FileInputStream inputStream = FileUtils.openInputStream(new File(OUTPUT_FILE_NAME))) {
				assertThat(inputStream).isNotNull();
				String stringDataSet = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
				System.out.println(stringDataSet);
				assertThat(stringDataSet).isNotNull()
				                         .isEqualTo(getExpectedJson());
			}
		}

		@Override
		public void beforeEach(ExtensionContext context) throws Exception {
			File file = new File(OUTPUT_FILE_NAME);
			Files.deleteIfExists(file.toPath());
		}

		private String getExpectedJson() throws IOException {
			try (InputStream inputStream = getClass().getResourceAsStream(INPUT_DATA_SET_FILE)) {
				return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			}
		}
	}
}
