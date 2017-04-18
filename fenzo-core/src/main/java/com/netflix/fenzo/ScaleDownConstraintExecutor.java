package com.netflix.fenzo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Executes multiple {@link ScaleDownConstraintEvaluator} evaluators, and combines the results using {@link ScaleDownOrderResolver}.
 */
class ScaleDownConstraintExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ScaleDownConstraintExecutor.class);

    private final List<ScaleDownConstraintEvaluator> evaluators;

    ScaleDownConstraintExecutor(List<ScaleDownConstraintEvaluator> evaluators) {
        this.evaluators = evaluators;
    }

    ScaleDownOrderResolver evaluate(Collection<VirtualMachineLease> candidates) {
        List<ScaleDownConstraintEvaluator.Result> results = new ArrayList<>();
        evaluators.forEach(evaluator -> {
            try {
                results.add(evaluator.evaluate(candidates));
            } catch (Exception e) {
                logger.info("Ignoring scale down constraint evaluator {} due to evaluation error", evaluator.getName(), e);
            }
        });
        ScaleDownOrderResolver resolver = new ScaleDownOrderResolver(results);
        if(logger.isDebugEnabled()) {
            logger.debug("Evaluation result={}", resolver.toCompactString());
        }
        return resolver;
    }
}
