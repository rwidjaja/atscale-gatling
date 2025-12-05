package com.atscale.java.xmla.simulations;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.utils.InjectionStepJsonUtil;
import com.atscale.java.xmla.XmlaProtocol;

import io.gatling.javaapi.core.ClosedInjectionStep;
import static io.gatling.javaapi.core.CoreDsl.constantConcurrentUsers;


@SuppressWarnings("unused")
public class AtScaleXmlaClosedInjectionStepSimulation extends AtScaleXmlaSimulation {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleXmlaClosedInjectionStepSimulation.class);

    public AtScaleXmlaClosedInjectionStepSimulation() {
        super();
        
        // Log important information for debugging
        LOGGER.info("Starting AtScaleXmlaClosedInjectionStepSimulation for model: {}", model);
        LOGGER.info("Steps JSON: {}", steps);
        
        List<ClosedStep> ClosedSteps = new ArrayList<>();
        try {
            ClosedSteps = InjectionStepJsonUtil.closedInjectionStepsFromJson(steps);
            LOGGER.info("Successfully parsed {} closed steps", ClosedSteps.size());
        } catch (Exception e) {
            LOGGER.error("Error parsing injection steps JSON: {}", steps, e);
        }
        
        List<ClosedInjectionStep> injectionSteps = new ArrayList<>();
        for (ClosedStep step : ClosedSteps) {
            if (step == null) {
                LOGGER.warn("Encountered null ClosedStep, skipping.");
                continue;
            }
            ClosedInjectionStep gatlingStep = step.toGatlingStep();
            if (gatlingStep == null) {
                LOGGER.warn("Failed to convert ClosedStep to Gatling step for: {}", step);
                continue;
            }
            injectionSteps.add(gatlingStep);
        }

        if (injectionSteps.isEmpty()) {
            LOGGER.warn("No valid injection steps provided. Defaulting to constantConcurrentUsers(1) for 1 minute");
            injectionSteps.add(constantConcurrentUsers(1).during(java.time.Duration.ofMinutes(1)));
        } else {
            LOGGER.info("Configured with {} injection steps", injectionSteps.size());
        }

        try {
            setUp(sb.injectClosed(injectionSteps).protocols(XmlaProtocol.forXmla(model)));
            LOGGER.info("Simulation setup complete for model: {}", model);
        } catch (Exception e) {
            LOGGER.error("Failed to set up simulation for model: {}", model, e);
            throw e;
        }
    }
}