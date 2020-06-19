package com.learn.springjms.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HelloWorldMessage implements Serializable {

    static final long serialVersionUID = -2998411295354972354L;

    private UUID id;
    private String message;
}
