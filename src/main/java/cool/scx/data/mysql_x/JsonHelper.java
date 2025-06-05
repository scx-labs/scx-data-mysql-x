package cool.scx.data.mysql_x;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.*;
import com.mysql.cj.xdevapi.*;
import cool.scx.common.util.ObjectUtils;
import cool.scx.data.field_policy.FieldPolicy;

/**
 * @author scx567888
 * @version 0.0.1
 */
class JsonHelper {

    final static ObjectMapper OBJECT_MAPPER = ObjectUtils.jsonMapper(new ObjectUtils.Options().setIgnoreJsonIgnore(true));

    final ObjectReader objectReader;

    JsonHelper(Class<?> c) {
        this.objectReader = OBJECT_MAPPER.readerFor(c);
    }

    public static JsonValue toJsonValue(JsonNode jsonNode) {
        if (jsonNode instanceof ObjectNode objectNode) {
            var dbDoc = new DbDocImpl();
            var fields = objectNode.fields();
            while (fields.hasNext()) {
                var next = fields.next();
                dbDoc.add(next.getKey(), toJsonValue(next.getValue()));
            }
            return dbDoc;
        } else if (jsonNode instanceof ArrayNode arrayNode) {
            var jsonValue = new JsonArray();
            for (var node : arrayNode) {
                jsonValue.add(toJsonValue(node));
            }
            return jsonValue;
        } else if (jsonNode instanceof NumericNode numericNode) {
            return new JsonNumber().setValue(numericNode.asText());
        } else if (jsonNode instanceof TextNode textNode) {
            return new JsonString().setValue(textNode.asText());
        } else if (jsonNode instanceof NullNode nullNode) {
            return JsonLiteral.NULL;
        } else if (jsonNode instanceof BooleanNode booleanNode) {
            boolean aBoolean = booleanNode.asBoolean();
            return aBoolean ? JsonLiteral.TRUE : JsonLiteral.FALSE;
        }
        throw new IllegalArgumentException("未知的 jsonNode 类型 !!!");
    }

    public static ObjectNode filterObjectNode(ObjectNode objectNode, FieldPolicy filter) {
        return switch (filter.getFilterMode()) {
            case EXCLUDED -> {
                for (String s : filter.getFieldNames()) {
                    objectNode.remove(s);
                }
                yield objectNode;
            }
            case INCLUDED -> {
                var newObjectNode = OBJECT_MAPPER.createObjectNode();
                for (var s : filter.getFieldNames()) {
                    newObjectNode.set(s, objectNode.get(s));
                }
                yield newObjectNode;
            }
        };
    }

    public static DbDoc filterDbDoc(DbDoc dbDoc, FieldPolicy filter) {
        return switch (filter.getFilterMode()) {
            case EXCLUDED -> {
                for (String s : filter.getFieldNames()) {
                    dbDoc.remove(s);
                }
                yield dbDoc;
            }
            case INCLUDED -> {
                var newDbDoc = new DbDocImpl();
                for (var s : filter.getFieldNames()) {
                    newDbDoc.put(s, dbDoc.get(s));
                }
                yield newDbDoc;
            }
        };
    }

    public static JsonNode toObjectNode(JsonValue jsonValue) {
        if (jsonValue instanceof DbDoc dbDoc) {
            var objectNode = OBJECT_MAPPER.createObjectNode();
            dbDoc.forEach((key, value) -> {
                objectNode.set(key, toObjectNode(value));
            });
            return objectNode;
        } else if (jsonValue instanceof JsonArray jsonArray) {
            var arrayNode = OBJECT_MAPPER.createArrayNode();
            for (var node : jsonArray) {
                arrayNode.add(toObjectNode(node));
            }
            return arrayNode;
        } else if (jsonValue instanceof JsonNumber jsonNumber) {
            return DecimalNode.valueOf(jsonNumber.getBigDecimal());
        } else if (jsonValue instanceof JsonString jsonString) {
            return TextNode.valueOf(jsonString.getString());
        } else if (jsonValue instanceof JsonLiteral jsonLiteral) {
            return switch (jsonLiteral) {
                case NULL -> NullNode.getInstance();
                case TRUE -> BooleanNode.TRUE;
                case FALSE -> BooleanNode.FALSE;
            };
        }
        throw new IllegalArgumentException("未知的 jsonValue 类型 !!!");
    }

}
