package com.barley.orleans;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by vikram.gulia on 4/6/16.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String filePath = "/Users/vikram.gulia/Documents/Admin/RatingEngineDocs/Examples Files/70107728_2015121404.txt";
        File file = new File(filePath);
        List<String> lines = Files.readAllLines(file.toPath(), Charset.defaultCharset());
        Map<String, Integer> timesPhoneCalled = new HashMap<>();
        for (String line : lines) {
            String callingGuy = line.substring(37, 37 + 15).trim();
            String dialedPh = line.substring(81, 81 + 15).trim();
            String calledNo = line.substring(96, 96 + 15).trim();
            String billCode = line.substring(75, 75 + 6).trim();
            String componentId = line.substring(292, 292 + 6).trim();

            if (dialedPh.length() > 10 || calledNo.length() > 10) {
                if (timesPhoneCalled.containsKey(dialedPh)) {
                    timesPhoneCalled.put(dialedPh, timesPhoneCalled.get(dialedPh) + 1);
                } else {
                    timesPhoneCalled.put(dialedPh, 0);
                }
                System.out.println("This num " + callingGuy + " dialed phone: " + dialedPh + ", called no: " + calledNo + ", billCode: " + billCode + ", with componentId: " + componentId);
            }
        }

        System.out.println("Times Phones called: " + timesPhoneCalled);
    }
}