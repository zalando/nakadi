package org.zalando.nakadi.util;

import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;

/*
 One could use JsonPath Lib instead: https://github.com/jayway/JsonPath

 However, I had the feeling that JsonPath is way too much compared to what is needed here. Therefore it might be
 slower. Moreover we already have a JSONObject. Using JsonPath would mean to convert it back to a string (or pass
 the json string instead of the JSONObject to the strategy) and parse it again.
 */
public class JsonPathAccess {
    private final JSONObject jsonObject;

    public JsonPathAccess(final JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public Object get(final String path) throws InvalidPartitionKeyFieldsException {

        final JsonPathTokenizer pathTokenizer = new JsonPathTokenizer(path);

        Object curr = this.jsonObject;
        String field;

        while ((field = pathTokenizer.nextToken()) != null) {
            if (!(curr instanceof JSONObject)) {
                throw new InvalidPartitionKeyFieldsException("field " + field + " doesn't exist.");
            }
            try {
                curr = ((JSONObject) curr).get(field);
            } catch (JSONException e) {
                throw new InvalidPartitionKeyFieldsException("field " + field + " doesn't exist.");
            }
        }
        return curr;
    }

    private static class JsonPathTokenizer {
        private final char[] path;
        private int pos = 0;
        private final StringBuilder tokenBuilder;

        private JsonPathTokenizer(final String path) {
            this.path = path.toCharArray();
            tokenBuilder = new StringBuilder(this.path.length);
        }

        public synchronized String nextToken() {
            if (pos >= path.length) {
                return null;
            }

            tokenBuilder.setLength(0);

            boolean inSingleQuote = false;
            boolean escaped = false;

            while (pos < path.length) {
                final char c = path[pos];

                pos++;

                if (escaped) {
                    tokenBuilder.append(c);
                    escaped = false;
                } else {

                    if (c == '\\') {
                        escaped = true;
                    } else if (c == '\'') {
                        inSingleQuote = !inSingleQuote;
                    } else if (c == '.' && !inSingleQuote) {
                        break;
                    } else {
                        tokenBuilder.append(c);
                    }
                }

            }

            if (tokenBuilder.length() > 0) {
                return tokenBuilder.toString();
            } else {
                return null;
            }
        }
    }
}
