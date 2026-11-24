package ipl.pdms.gateway.selection;

import static ipl.pdms.gateway.selection.SelectionModel.ARCHITECTURES;
import static ipl.pdms.gateway.selection.SelectionModel.DECLARED_CAPABILITIES;
import static java.lang.Boolean.TRUE;
import static java.util.Comparator.comparingDouble;

import ipl.pdms.gateway.routing.RoutingSnapshot;
import ipl.pdms.gateway.selection.SelectionModel.Candidate;
import ipl.pdms.gateway.selection.SelectionModel.Configuration;
import ipl.pdms.gateway.selection.SelectionModel.Decision;
import ipl.pdms.gateway.selection.SelectionModel.Signals;
import ipl.pdms.gateway.selection.SelectionModel.Tuning;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public final class SelectionPolicy {

    private long lastTick = Long.MIN_VALUE;
    private long lastSwitch = Long.MIN_VALUE;
    private String lastActive;
    private int breaches;
    private int opportunities;
    private String opportunityTarget;
    private final ArrayDeque<Long> switches = new ArrayDeque<>();

    public void resetEvidence() {
        lastTick = Long.MIN_VALUE;
        breaches = opportunities = 0;
        opportunityTarget = null;
    }

    public void applied(long now) {
        lastSwitch = now;
        switches.addLast(now);
        resetEvidence();
    }

    public Decision evaluate(Configuration config, Signals signal, RoutingSnapshot routing, long now) {
        final var tuning = config.tuning();
        final var active = routing.active();
        final boolean activeErrors = signal.unexpectedErrorRatio() != null
            && signal.unexpectedErrorRatio() > tuning.highErrorRatio() && freshSignal(signal, tuning, now);
        prepareEvaluation(active, tuning, now);
        final var candidates = evaluateCandidates(config, signal, routing, activeErrors);
        final boolean forced = !TRUE.equals(signal.healthy().get(active)) || activeErrors
            || !DECLARED_CAPABILITIES.getOrDefault(active, java.util.Set.of()).containsAll(config.requiredCapabilities());
        final var holdReason = observationHoldReason(config, signal, forced, now);
        if (holdReason != null) {
            return hold(config, now, active, holdReason, candidates);
        }
        lastTick = now;
        final var best = candidates.stream().filter(Candidate::eligible)
            .min(comparingDouble((Candidate c) -> c.predictedP99Ms())
                .thenComparing(c -> !c.architecture().equals(active))
                .thenComparingInt(c -> ARCHITECTURES.indexOf(c.architecture())))
            .orElse(null);
        if (best == null) {
            breaches = opportunities = 0;
            return decision(config, now, active, null, "hold", "no_eligible_candidate", candidates);
        }
        if (forced) {
            return decision(config, now, active, best.architecture(), "switch", "availability_or_requirement", candidates);
        }
        return evaluatePerformance(config, signal, active, best, candidates, now);
    }

    private void prepareEvaluation(String active, Tuning tuning, long now) {
        if (!active.equals(lastActive)) {
            resetEvidence();
            lastActive = active;
        }
        while (!switches.isEmpty() && now - switches.peekFirst() >= tuning.flapWindowMs()) {
            switches.removeFirst();
        }
    }

    private String observationHoldReason(Configuration config, Signals signal, boolean forced, long now) {
        final var tuning = config.tuning();
        if (!config.enabled()) {
            return "disabled";
        }
        if (signal.gatewayPressure()) {
            resetEvidence();
            return "gateway_pressure";
        }
        if (!forced) {
            if (!freshSignal(signal, tuning, now)) {
                resetEvidence();
                return "stale_or_invalid_signal";
            }
            if (signal.samples() < tuning.minSamples() || signal.p99Ms() == null) {
                resetEvidence();
                return "insufficient_samples";
            }
            if (lastTick != Long.MIN_VALUE && now - lastTick < tuning.tickMs()) {
                return "tick_not_due";
            }
        }
        return null;
    }

    private boolean freshSignal(Signals signal, Tuning tuning, long now) {
        return signal.valid() && signal.sampledAtEpochMs() <= now && now - signal.sampledAtEpochMs() <= tuning.maxAgeMs();
    }

    private Decision evaluatePerformance(Configuration config, Signals signal, String active, Candidate best,
                                         List<Candidate> candidates, long now) {
        final var tuning = config.tuning();
        final var current = candidates.stream().filter(c -> c.architecture().equals(active)).findFirst().orElse(null);
        if (current != null && (current.reason().equals("insufficient_headroom")
            || current.reason().equals("outside_profile_support") && current.utilization() != null
            && current.utilization() > tuning.maxUtilization())) {
            breaches = opportunities = 0;
            opportunityTarget = null;
            return switchOrHold(config, now, active, best.architecture(), "headroom", candidates);
        }
        if (current == null || current.predictedP99Ms() == null || current.reason().equals("synthetic_profile")
            || current.reason().equals("outside_profile_support")) {
            breaches = opportunities = 0;
            return hold(config, now, active, "active_profile_unknown", candidates);
        }
        if (signal.p99Ms() > tuning.highP99Ms()) {
            breaches++;
        } else if (signal.p99Ms() <= tuning.recoveryP99Ms()) {
            breaches = 0;
        }
        final boolean improvement = !best.architecture().equals(active)
            && current.predictedP99Ms() - best.predictedP99Ms() >= tuning.improvementMs()
            && best.predictedP99Ms() <= current.predictedP99Ms() * (1 - tuning.improvementFraction());
        recordOpportunity(best.architecture(), improvement);
        if (best.architecture().equals(active)) {
            return hold(config, now, active, "keep_active", candidates);
        }
        if (!improvement) {
            return hold(config, now, active, "improvement_too_small", candidates);
        }
        if (breaches < tuning.breachTicks() && opportunities < tuning.opportunityTicks()) {
            return hold(config, now, active, "debounce", candidates);
        }
        return switchOrHold(config, now, active, best.architecture(),
            breaches >= tuning.breachTicks() ? "latency_breach" : "opportunity", candidates);
    }

    private void recordOpportunity(String target, boolean improvement) {
        if (improvement && target.equals(opportunityTarget)) {
            opportunities++;
        } else {
            opportunities = improvement ? 1 : 0;
            opportunityTarget = improvement ? target : null;
        }
    }

    private Decision switchOrHold(Configuration config, long now, String active, String target,
                                  String reason, List<Candidate> candidates) {
        final var tuning = config.tuning();
        if (lastSwitch != Long.MIN_VALUE && now - lastSwitch < tuning.dwellMs()) {
            return hold(config, now, active, "cooldown", candidates);
        }
        if (switches.size() >= tuning.maxSwitches()) {
            return hold(config, now, active, "flap_budget", candidates);
        }
        return decision(config, now, active, target, "switch", reason, candidates);
    }

    private Decision hold(Configuration config, long now, String active, String reason, List<Candidate> candidates) {
        return decision(config, now, active, active, "hold", reason, candidates);
    }

    private List<Candidate> evaluateCandidates(Configuration config,
                                               Signals signal,
                                               RoutingSnapshot routing,
                                               boolean activeErrors) {
        final var active = routing.active();
        final var t = config.tuning();
        final List<Candidate> candidates = new ArrayList<>();
        for (final String name : ARCHITECTURES) {
            final var profile = config.profiles().getOrDefault(name, java.util.Map.of()).get(config.workload());
            final Double utilization = profile == null ? null : signal.arrivalRps() / profile.capacityRps();
            String reason = "eligible";
            if (!routing.knows(name) || !routing.isEnabled(name)) {
                reason = "disabled";
            } else if (!TRUE.equals(signal.healthy().get(name))) {
                reason = "unhealthy";
            } else if (name.equals(active) && activeErrors) {
                reason = "active_errors";
            } else if (!DECLARED_CAPABILITIES.get(name).containsAll(config.requiredCapabilities())) {
                reason = "missing_capability";
            } else if (profile == null) {
                reason = "profile_unknown";
            } else if (config.source().equals("real") && profile.evidenceKind().equals("synthetic")) {
                reason = "synthetic_profile";
            } else if (signal.arrivalRps() < profile.minRateRps() || signal.arrivalRps() > profile.maxRateRps()) {
                reason = "outside_profile_support";
            } else if (utilization > t.maxUtilization()) {
                reason = "insufficient_headroom";
            }
            candidates.add(new Candidate(name, reason.equals("eligible"), reason,
                profile == null ? null : profile.predictedP99Ms(), utilization, profile == null ? null : profile.evidenceKind()));
        }
        return candidates;
    }

    private Decision decision(Configuration c,
                              long now,
                              String active,
                              String target,
                              String action,
                              String reason,
                              List<Candidate> candidates) {
        return new Decision(
            action,
            reason,
            active,
            target,
            c.source(),
            c.source().equals("mock") ? "aggregate_fixture" : "live_observation",
            now,
            List.copyOf(candidates),
            breaches,
            opportunities);
    }
}
