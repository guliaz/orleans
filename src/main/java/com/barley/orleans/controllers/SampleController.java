package com.barley.orleans.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sample")
public class SampleController {
    @RequestMapping("/")
    @ResponseBody
    String home() {
        return "Hello World!!";
    }
}
