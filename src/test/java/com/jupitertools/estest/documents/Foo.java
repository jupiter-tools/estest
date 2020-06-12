package com.jupitertools.estest.documents;


import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Document(indexName = "foo-index")
public class Foo {
    @Id
    private String id;
    private Date time;
    private int counter;
}