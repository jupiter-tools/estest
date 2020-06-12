package com.jupitertools.estest.annotation;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ElasticDataSet {

	/**
	 * Path to a file with the data-set for populating
	 */
	String value() default "";

	/**
	 * Clean a MongoDB database before the test execution
	 */
	boolean cleanBefore() default false;

	/**
	 * Clean a MongoDB database after the test execution
	 */
	boolean cleanAfter() default false;

	/**
	 * expected unmodifiable data set in this test case
	 */
	boolean readOnly() default false;
}
