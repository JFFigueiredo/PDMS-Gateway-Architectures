package ipl.pdms.gateway.selection;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static ipl.pdms.gateway.selection.SelectionModel.DECLARED_CAPABILITIES;
import static ipl.pdms.gateway.selection.SelectionModel.WORKLOADS;
import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import java.util.Map;
import java.util.TreeMap;

public final class SelectionPolicyVersion {
    private static final ObjectMapper JSON = new ObjectMapper();
    private SelectionPolicyVersion() {}

    public static JsonNode document(Configuration configuration, boolean allowApply) {
        final Map<String, Object> capabilities = new TreeMap<>();
        DECLARED_CAPABILITIES.forEach((name, values) -> capabilities.put(name, values.stream().sorted().toList()));
        final Map<String, Object> fields = new TreeMap<>();
        fields.put("contract", "pdms-selection-policy/1");
        fields.put("architectureOrder", ARCHITECTURES);
        fields.put("workloads", WORKLOADS.stream().sorted().toList());
        fields.put("capabilities", capabilities);
        fields.put("enabled", configuration.enabled());
        fields.put("autoEvaluate", configuration.autoEvaluate());
        fields.put("applyChanges", configuration.applyChanges());
        fields.put("allowApplyAtStartup", allowApply);
        fields.put("source", configuration.source());
        fields.put("profiles", configuration.profiles());
        fields.put("tuning", configuration.tuning());
        return ordered(JSON.valueToTree(fields), JSON);
    }

    private static JsonNode ordered(JsonNode value, ObjectMapper mapper) {
        if (value.isObject()) {
            final ObjectNode result = mapper.createObjectNode();
            final var names = new java.util.ArrayList<String>();
            value.fieldNames().forEachRemaining(names::add);
            names.stream().sorted().forEach(name -> result.set(name, ordered(value.get(name), mapper)));
            return result;
        }
        if (value.isArray()) {
            final var result = mapper.createArrayNode();
            value.forEach(item -> result.add(ordered(item, mapper)));
            return result;
        }
        return value;
    }

    public static String sha256(Configuration configuration, boolean allowApply) {
        try {
            final byte[] bytes = JSON.writeValueAsBytes(document(configuration, allowApply));
            return sha256Hex(bytes);
        } catch (java.io.IOException error) {
            throw new IllegalStateException("Cannot fingerprint selection policy", error);
        }
    }
}
