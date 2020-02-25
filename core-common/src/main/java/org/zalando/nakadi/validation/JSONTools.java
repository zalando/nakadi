package org.zalando.nakadi.validation;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class JSONTools {

    public static void extendJson(final JSONObject toUpdate, final JSONObject toInject) {
        for (final String key : toInject.keySet()) {
            if (toUpdate.has(key)) {
                final Object oldValue = toUpdate.get(key);
                final Object newValue = toInject.get(key);
                if (oldValue.getClass() != newValue.getClass()) {
                    throw new RuntimeException("Only same type json merge is allowed, " + oldValue +
                            " and " + newValue + " are not mergable");
                }
                if (oldValue instanceof JSONObject) {
                    extendJson((JSONObject)oldValue, (JSONObject)newValue);
                } else if (oldValue instanceof JSONArray) {
                    final JSONArray oldJsonArray = (JSONArray) oldValue;
                    final JSONArray newJsonArray = (JSONArray) newValue;
                    // Only extension is supported
                    for (int idx = 0; idx < newJsonArray.length(); ++idx) {
                        oldJsonArray.put(newJsonArray.get(idx));
                    }
                } else {
                    toUpdate.put(key, toInject.get(key));
                }
            } else {
                toUpdate.put(key, toInject.get(key));
            }
        }
    }

    public static void replaceAll(final JSONArray array, final String toRemove, final String toAdd) {
        for (int i = 0; i < array.length(); ++i) {
            final Object o = array.get(i);
            if (o instanceof String) {
                if (o.equals(toRemove)) {
                    array.put(i, toAdd);
                }
            } else if (o instanceof JSONArray) {
                replaceAll((JSONArray) o, toRemove, toAdd);
            } else if (o instanceof JSONObject) {
                replaceAll((JSONObject) o, toRemove, toAdd);
            }
        }
    }

    public static void replaceAll(final JSONObject obj, final String toRemove, final String toAdd) {
        final List<String> keys = new ArrayList<>(obj.keySet()); // To avoid concurrent modification exception.
        for (final String key : keys) {
            final Object o = obj.get(key);
            if (o instanceof String) {
                if (o.equals(toRemove)) {
                    obj.put(key, toAdd);
                }
            } else if (o instanceof JSONObject) {
                replaceAll((JSONObject) o, toRemove, toAdd);
            } else if (o instanceof JSONArray) {
                replaceAll((JSONArray) o, toRemove, toAdd);
            }
        }
    }

}
