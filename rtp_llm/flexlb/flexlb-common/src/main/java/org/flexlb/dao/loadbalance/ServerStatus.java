package org.flexlb.dao.loadbalance;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;
import org.flexlb.dao.route.RoleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@ToString(exclude = {"rejections", "endpointFeasibilities"})
public class ServerStatus {
    @JsonProperty("role")
    private RoleType role;

    @JsonProperty("server_ip")
    private String serverIp;

    @JsonProperty("http_port")
    private int httpPort;

    @JsonProperty("grpc_port")
    private int grpcPort;

    @JsonProperty("dp_rank")
    private long dpRank;

    @JsonProperty("prefill_time")
    private long prefillTime;

    @JsonProperty("group")
    private String group;

    @JsonProperty("debug_info")
    private DebugInfo debugInfo;

    @JsonProperty("request_id")
    private long requestId;

    @JsonProperty("success")
    private boolean success;

    @JsonProperty("code")
    private int code;

    @JsonProperty("message")
    private String message;

    @JsonIgnore
    private Map<RejectionReason, Integer> rejections = Collections.emptyMap();

    @JsonIgnore
    private List<EndpointFeasibility> endpointFeasibilities = Collections.emptyList();

    public static ServerStatus code(StrategyErrorType code) {
        ServerStatus result = new ServerStatus();
        result.setSuccess(false);
        result.setCode(code.getErrorCode());
        result.setMessage(code.getErrorMsg());
        return result;
    }

    /**
     * Create a failure ServerStatus with rejection details for eviction routing.
     *
     * @param code       the strategy error type
     * @param rejections the rejection reason&rarr;count map from the filter stage
     * @return a failure ServerStatus
     */
    public static ServerStatus failure(StrategyErrorType code, Map<RejectionReason, Integer> rejections) {
        ServerStatus result = code(code);
        result.setRejections(rejections);
        return result;
    }

    /**
     * Create a failure ServerStatus with per-endpoint feasibility details for
     * the v4 cost-based eviction planner.
     *
     * <p>The {@code rejections} summary map is derived on-demand from the
     * feasibilities list via {@link #getRejections()} (grouped by reason,
     * counting endpoints per reason).
     *
     * @param code          the strategy error type
     * @param feasibilities per-endpoint feasibility assessments
     * @return a failure ServerStatus
     */
    public static ServerStatus failureWithFeasibility(StrategyErrorType code, List<EndpointFeasibility> feasibilities) {
        ServerStatus status = code(code);
        status.setEndpointFeasibilities(feasibilities);
        return status;
    }

    /**
     * Returns the rejection reason&rarr;count map.
     *
     * <p>If {@code endpointFeasibilities} is non-empty, the map is derived from
     * the feasibilities (grouped by reason, counting endpoints per reason).
     * Otherwise, returns the stored {@code rejections} field.
     *
     * @return rejection summary map
     */
    public Map<RejectionReason, Integer> getRejections() {
        if (endpointFeasibilities != null && !endpointFeasibilities.isEmpty()) {
            return endpointFeasibilities.stream()
                    .collect(Collectors.groupingBy(
                            EndpointFeasibility::reason,
                            Collectors.summingInt(e -> 1)));
        }
        return rejections;
    }
}
