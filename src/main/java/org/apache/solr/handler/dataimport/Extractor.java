package org.apache.solr.handler.dataimport;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Extractor {

    private static final Pattern EXTRACT_PATTERN = Pattern.compile("(?<prop>.*)\\[(?<arrIndex>\\d+)]");

    private Extractor next;
    private String propName;
    private Integer arrayIndex;

    public Extractor(String propName, Extractor next) {
        this.next = next;

        Matcher matcher = EXTRACT_PATTERN.matcher(propName);
        if (matcher.find()) {
            this.propName = matcher.group("prop");
            this.arrayIndex = Integer.parseInt(matcher.group("arrIndex"));
        } else {
            this.propName = propName;
        }
    }

    public Object extract(Map<String, Object> map) {

        Object result = map.get(propName);

        if (result == null) return null;
        if (result instanceof List) result = ((List) result).get(arrayIndex);

        return (next != null && result instanceof Map) ? next.extract((Map) result) : result;
    }
}
