package org.flexlb.balance.policy;

import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.FlexlbRequest;
import org.springframework.stereotype.Component;

@Component
public class ConfigurableGroupRoutingPolicy implements GroupRoutingPolicy {

    private static final String POLICY_NAME = "trafficPolicy";

    private final ConfigService configService;

    public ConfigurableGroupRoutingPolicy(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public GroupRoutingDecision route(FlexlbRequest request) {
        FlexlbConfig config = request.getConfig() != null ? request.getConfig() : configService.loadBalanceConfig();
        if (config == null || config.getTrafficPolicy() == null) {
            return GroupRoutingDecision.none();
        }

        return config.getTrafficPolicy()
                .resolveTargetGroup(request.getRequest())
                .map(group -> GroupRoutingDecision.of(group, POLICY_NAME))
                .orElseGet(GroupRoutingDecision::none);
    }
}
