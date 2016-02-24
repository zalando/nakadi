package de.zalando.aruha.nakadi.util;

import de.zalando.aruha.nakadi.partitioning.InvalidOrderingKeyFieldsException;
import org.json.JSONException;
import org.json.JSONObject;

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

    public Object get(final String path) throws InvalidOrderingKeyFieldsException {

        final JsonPathTokenizer pathTokenizer = new JsonPathTokenizer(path);

        Object curr = this.jsonObject;
        String field;

        while ((field = pathTokenizer.nextToken()) != null) {
            if (!(curr instanceof JSONObject)) {
                throw new InvalidOrderingKeyFieldsException("field " + field + "doesn't exist.");
            }
            try {
                curr = ((JSONObject) curr).get(field);
            } catch (JSONException e) {
                throw new InvalidOrderingKeyFieldsException("field " + field + "doesn't exist.");
            }
        }
        return curr;
    }

    private static class JsonPathTokenizer {
        private final char[] path;
        private int pos = 0;

        private JsonPathTokenizer(final String path) {
            this.path = path.toCharArray();
        }

        public synchronized String nextToken() {
            if (pos >= path.length) {
                return null;
            }

            //int oldPos = pos;
            StringBuilder tokenBuilder = new StringBuilder(path.length - pos);

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

            final String token = tokenBuilder.toString();
            if (token.length() > 0) {
                return token;
            } else {
                return null;
            }
        }
    }
}
