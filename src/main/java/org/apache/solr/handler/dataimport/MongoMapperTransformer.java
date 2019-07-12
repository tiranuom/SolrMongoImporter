package org.apache.solr.handler.dataimport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Created by IntelliJ IDEA.
 * User: James
 * Date: 15/08/12
 * Time: 13:52
 * To change this template use File | Settings | File Templates.
 */
public class MongoMapperTransformer extends Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoMapperTransformer.class);

    public static final String MONGO_FIELD = "mongoField";

    private Map<String, Extractor> extractorMap = new HashMap<>();

    @Override
    public Object transformRow(Map<String, Object> row, Context context) {

        for (Map<String, String> map : context.getAllEntityFields()) {
            String mongoFieldName = map.get(MONGO_FIELD);
            if (mongoFieldName == null)
                continue;

            Extractor extractor = extractorMap.computeIfAbsent(mongoFieldName, this::generateExtractor);

            String columnFieldName = map.get(DataImporter.COLUMN);

            row.put(columnFieldName, extractor.extract(row));
        }

        LOGGER.info("Extracted row [{}]", row);

        return row;
    }

    private Extractor generateExtractor(String mongoFieldName) {

        String[] parts = mongoFieldName.split("\\.");

        Extractor extractor = null;

        for (int i = parts.length - 1; i >= 0; i--) {
            String part = parts[i];
            extractor = new Extractor(part, extractor);
        }
        return extractor;
    }
}
