package com.barley.orleans.metrics.meters;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class AppMeters {

    static final MetricRegistry metrics = new MetricRegistry();

    public AppMeters() {
        final JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
    }

    private final Map<String, Meter> meters = new HashMap<>();

    public void increment(String name) {
        if (meters.containsKey(name)) {
            meters.get(name).mark();
        } else {
            final Meter requests = metrics.meter(name);
            meters.put(name, requests);
            requests.mark();
        }
    }


}
