/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.ServerHealthComponents.SNAPSHOT;

/**
 * This indicator reports health for snapshot lifecycle management component.
 *
 * Indicator will report YELLOW status when SLM is not running and there are configured policies.
 * Data might not be backed up timely in such cases.
 *
 * SLM must be running to fix warning reported by this indicator.
 */
public class SlmHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "slm";

    public static final String HELP_URL = "https://ela.st/fix-slm";
    public static final UserAction SLM_NOT_RUNNING = new UserAction(
        new UserAction.Definition("slm-not-running", "Start SLM using [POST /_slm/start].", HELP_URL),
        null
    );

    private final ClusterService clusterService;

    public SlmHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return SNAPSHOT;
    }

    @Override
    public String helpURL() {
        return HELP_URL;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        var slmMetadata = clusterService.state().metadata().custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
        if (slmMetadata.getSnapshotConfigurations().isEmpty()) {
            return createIndicator(
                GREEN,
                "No SLM policies configured",
                createDetails(explain, slmMetadata),
                Collections.emptyList(),
                Collections.emptyList()
            );
        } else if (slmMetadata.getOperationMode() != OperationMode.RUNNING) {
            List<HealthIndicatorImpact> impacts = Collections.singletonList(
                new HealthIndicatorImpact(
                    3,
                    "Scheduled snapshots are not running. New backup snapshots will not be created automatically.",
                    List.of(ImpactArea.BACKUP)
                )
            );
            return createIndicator(YELLOW, "SLM is not running", createDetails(explain, slmMetadata), impacts, List.of(SLM_NOT_RUNNING));
        } else if(slmMetadata.getSnapshotConfigurations().isEmpty() == false) {

            //This is not the ideal implementation since it would create an indicator based on the first occurrence and the first only.
            //That said it seems like it would be good to have an integration between the Health Service and the SnapshotLifecycle stuff.
            //But I deem myself too much ignorant about the whole project to affirm it with certainty.
            for (Map.Entry<String, SnapshotLifecyclePolicyMetadata> entry : slmMetadata.getSnapshotConfigurations().entrySet())
            {
                SnapshotLifecyclePolicyMetadata policy = entry.getValue();
                if (policy != null)
                {
                    //This could be configurable by the SnapshotLifecyclePolicy.java constructor,
                    //But I didn't have time to do such a big change, since it would affect a lot of schedule tasks
                    //on already existent snapshots.
                    long redNoRecentSuccessThreshold = 7889400000L; // 3 months in milliseconds;
                    long yellowNoRecentSuccessThreshold = 2400000L;// 40 minutes in milliseconds
                    long lastSuccess = policy.getLastSuccess().getSnapshotStartTimestamp();
                    long lastFailure = policy.getLastFailure().getSnapshotFinishTimestamp();
                    long timeDifference = lastFailure - lastSuccess;
                    if (timeDifference > redNoRecentSuccessThreshold) {

                        return createIndicator(
                            RED,
                            "The following configured snapshot policy's lastSucess time, exceeds the threshold of "
                                + redNoRecentSuccessThreshold + " milliseconds: " + policy.getName(),
                            createDetails(explain, slmMetadata),
                            Collections.emptyList(),
                            Collections.emptyList()

                        );
                    }
                    else if(timeDifference > yellowNoRecentSuccessThreshold)
                    {
                        return createIndicator(
                            YELLOW,
                            "The following configured snapshot policy's lastSucess time, exceeds the threshold of "
                                + yellowNoRecentSuccessThreshold + " milliseconds: " + policy.getName(),
                            createDetails(explain, slmMetadata),
                            Collections.emptyList(),
                            Collections.emptyList()

                        );
                    }
                }
            }

        }
        return createIndicator(
            GREEN,
            "SLM is running",
            createDetails(explain, slmMetadata),
            Collections.emptyList(),
            Collections.emptyList()
        );
    }

    private static HealthIndicatorDetails createDetails(boolean explain, SnapshotLifecycleMetadata metadata) {
        if (explain) {
            return new SimpleHealthIndicatorDetails(
                Map.of("slm_status", metadata.getOperationMode(), "policies", metadata.getSnapshotConfigurations().size())
            );
        } else {
            return HealthIndicatorDetails.EMPTY;
        }
    }
}
