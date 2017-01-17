package org.zalando.nakadi.stream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static String getObjectFromJsonPath(final String jsonPath, final String json) {
        try {
            JSONObject jsonObject = new JSONObject(json);
            final String[] keys = jsonPath.split("\\.");
            for (int i = 0; i < keys.length; i++) {
                if (i == keys.length - 1)
                    return String.valueOf(jsonObject.get(keys[i]));
                final Object obj = jsonObject.get(keys[i]);
                if (obj instanceof JSONObject)
                    jsonObject = jsonObject.getJSONObject(keys[i]);
                if (obj instanceof JSONArray)
                    return jsonObject.getJSONArray(keys[i]).toString();
            }
        } catch (final Throwable th) {
            LOG.debug(th.getMessage(), th);
        }
        return null;
    }
}
