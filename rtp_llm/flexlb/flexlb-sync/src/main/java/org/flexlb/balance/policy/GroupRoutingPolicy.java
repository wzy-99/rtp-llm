package org.flexlb.balance.policy;

import org.flexlb.dao.FlexlbRequest;

public interface GroupRoutingPolicy {

    GroupRoutingDecision route(FlexlbRequest request);
}
