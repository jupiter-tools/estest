package com.jupitertools.estest.annotation;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Populate data in the ElasticSearch before run a test case.
 * This annotation required {@link com.jupitertools.estest.junit5.ElasticDataSetExtension}
 * you can declare this directly by {@link org.junit.jupiter.api.extension.ExtendWith} or
 * use the {@link EnableElasticDataSet} in your tests.
 *
 * @author Korovin Anatoliy
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ElasticDataSet {

	/**
	 * Path to a file with the data-set for populating
	 */
	String value() default "";

	/**
	 * Clean an Elasticsearch database before the test execution
	 */
	boolean cleanBefore() default false;

	/**
	 * Clean an ElasticSearch database after the test execution
	 */
	boolean cleanAfter() default false;

	/**
	 * Expected no modification a database while run test case
	 */
	boolean readOnly() default false;
}
