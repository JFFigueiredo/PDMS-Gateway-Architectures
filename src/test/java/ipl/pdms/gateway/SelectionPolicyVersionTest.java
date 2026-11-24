package ipl.pdms.gateway;

import static ipl.pdms.gateway.selection.SelectionPolicyVersion.sha256;
import static java.util.Comparator.reverseOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionModel.MockSignals;
import ipl.pdms.gateway.selection.SelectionModel.Profile;
import ipl.pdms.gateway.selection.SelectionModel.Tuning;
import ipl.pdms.gateway.selection.SelectionScenarios;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SelectionPolicyVersionTest {
    private Configuration scenario() {
        return SelectionScenarios.create("read-crossover", "monolith", 1000);
    }
    private String hash(Configuration value) {
        return sha256(value, false);
    }
    private Configuration copy(Configuration c, String source, Map<String, Map<String, Profile>> profiles, Tuning tuning) {
        return new Configuration(c.enabled(), c.autoEvaluate(), c.applyChanges(), source, c.workload(),
                c.requiredCapabilities(), profiles, tuning, c.mock());
    }

    @Test void profileMapOrderDoesNotChangePolicyIdentity() {
        Configuration c = scenario();
        Map<String, Map<String, Profile>> reversed = new LinkedHashMap<>();
        c.profiles().keySet().stream().sorted(reverseOrder()).forEach(name -> {
            Map<String, Profile> workloads = new LinkedHashMap<>();
            c.profiles().get(name).keySet().stream().sorted(reverseOrder())
                    .forEach(workload -> workloads.put(workload, c.profiles().get(name).get(workload)));
            reversed.put(name, workloads);
        });
        assertEquals(hash(c), hash(copy(c, c.source(), reversed, c.tuning())));
        assertTrue(hash(c).matches("[0-9a-f]{64}"));
    }

    @Test void observationsAndScenarioInputsDoNotChangePolicyIdentity() {
        Configuration c = scenario();
        Configuration inputs = new Configuration(c.enabled(), c.autoEvaluate(), c.applyChanges(), c.source(),
                "write", Set.of("consent_integrity_anchor"), c.profiles(), c.tuning(),
                new MockSignals(99999, 4, 900.0, 2, 50, false, Map.of(), true));
        assertEquals(hash(c), hash(inputs));
    }

    @Test void thresholdsProfileEvidenceAndExecutionModesChangePolicyIdentity() {
        Configuration c = scenario(); Tuning t = c.tuning();
        Tuning changed = new Tuning(t.tickMs(), t.windowSeconds(), t.minSamples(), t.maxAgeMs(),
                t.highP99Ms() + 1, t.recoveryP99Ms(), t.breachTicks(), t.opportunityTicks(), t.dwellMs(),
                t.maxUtilization(), t.improvementFraction(), t.improvementMs(), t.maxSwitches(), t.flapWindowMs(), t.highErrorRatio());
        assertNotEquals(hash(c), hash(copy(c, c.source(), c.profiles(), changed)));
        assertNotEquals(hash(c), hash(copy(c, "real", c.profiles(), c.tuning())));
        Map<String, Map<String, Profile>> profiles = new LinkedHashMap<>(c.profiles());
        Map<String, Profile> read = new LinkedHashMap<>(profiles.get("monolith")); Profile p = read.get("read");
        read.put("read", new Profile(p.evidenceKind(), "different-provenance", p.minRateRps(), p.maxRateRps(), p.capacityRps(), p.predictedP99Ms()));
        profiles.put("monolith", read);
        assertNotEquals(hash(c), hash(copy(c, c.source(), profiles, c.tuning())));
        assertNotEquals(hash(c), sha256(c, true));
        for (Configuration mode : java.util.List.of(
                new Configuration(!c.enabled(), c.autoEvaluate(), c.applyChanges(), c.source(), c.workload(), c.requiredCapabilities(), c.profiles(), c.tuning(), c.mock()),
                new Configuration(c.enabled(), !c.autoEvaluate(), c.applyChanges(), c.source(), c.workload(), c.requiredCapabilities(), c.profiles(), c.tuning(), c.mock()),
                new Configuration(c.enabled(), c.autoEvaluate(), !c.applyChanges(), c.source(), c.workload(), c.requiredCapabilities(), c.profiles(), c.tuning(), c.mock()))) {
            assertNotEquals(hash(c), hash(mode));
        }
    }
}
