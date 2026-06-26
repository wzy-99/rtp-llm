#include "autil/TimeUtility.h"
#include "rtp_llm/cpp/model_rpc/QueryConverter.h"
#include "rtp_llm/cpp/model_rpc/PrefillRpcServer.h"
#include "rtp_llm/cpp/utils/DebugUtils.h"
#include "rtp_llm/cpp/utils/HashUtil.h"
#include "rtp_llm/cpp/config/ConfigModules.h"
#include "rtp_llm/cpp/engine_base/Host.h"
#include "rtp_llm/cpp/utils/ProfilingScope.h"
#include "rtp_llm/cpp/models/logits_processor/LogitsProcessorFactory.h"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <cstring>
#include <functional>
#include <strings.h>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <unistd.h>
#include <limits.h>
#include <cstdint>
#include <map>

using namespace std;
using namespace autil::legacy;

using grpc::Status;
using grpc::ClientContext;

namespace rtp_llm {

PrefillRpcServer::~PrefillRpcServer() {
    stopAsyncResponseWorkers();
    stopResponseRegistryGc();
    if (enqueue_worker_pool_) {
        enqueue_worker_pool_->stop();
        enqueue_worker_pool_.reset();
    }
    if (worker_lambda_pool_) {
        worker_lambda_pool_->stop();
        worker_lambda_pool_.reset();
    }
    if (slot_worker_pool_) {
        slot_worker_pool_->stop();
        slot_worker_pool_.reset();
    }
}

namespace {

bool envValueIsTrue(const char* value) {
    return value != nullptr
           && (strcmp(value, "1") == 0 || strcasecmp(value, "true") == 0 || strcasecmp(value, "on") == 0
               || strcasecmp(value, "yes") == 0);
}

bool envValueIsFalse(const char* value) {
    return value != nullptr
           && (strcmp(value, "0") == 0 || strcasecmp(value, "false") == 0 || strcasecmp(value, "off") == 0
               || strcasecmp(value, "no") == 0);
}

bool prefillTraceLogEnabled() {
    static const bool enabled = []() {
        const char* value = std::getenv("PREFILL_TRACE_LOG_ENABLE");
        if (value == nullptr) {
            value = std::getenv("PREFILL_CACHE_DEBUG_LOG");
        }
        if (value == nullptr) {
            value = std::getenv("KV_CACHE_DEBUG_LOG");
        }
        return envValueIsTrue(value);
    }();
    return enabled;
}

// Helper to detect whether a Future (std::future or autil Future) is ready.
// Uses SFINAE: std::future::wait_for returns std::future_status; autil
// Future::wait_for also returns std::future_status.
template<typename FutureT>
bool futureIsReady(FutureT& f) {
    return f.valid() && f.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
}

template<typename FutureT>
void detachLeftoverFutures(std::vector<FutureT>& futures) {
    // Clear leftover futures naturally; no longer creates a throwaway thread.
    // Futures that are not yet ready are simply abandoned — the thread pool
    // owns the actual work; the future object is just a handle.
    futures.clear();
}

template<typename FutureT>
void drainReadyFutures(std::vector<FutureT>& futures, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        bool all_done  = true;
        bool any_ready = false;
        for (auto& f : futures) {
            if (f.valid()) {
                if (futureIsReady(f)) {
                    try {
                        f.get();
                    } catch (...) {}
                    any_ready = true;
                } else {
                    all_done = false;
                }
            }
        }
        if (all_done)
            break;
        if (!any_ready) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

template<typename FutureT, typename OnReady, typename OnTimeout>
void collectFutures(std::vector<FutureT>&                 futures,
                    std::chrono::steady_clock::time_point deadline,
                    OnReady&&                             on_ready,
                    OnTimeout&&                           on_timeout) {
    std::vector<bool> collected(futures.size(), false);
    size_t            remaining = futures.size();
    while (remaining > 0 && std::chrono::steady_clock::now() < deadline) {
        bool any_ready = false;
        for (size_t i = 0; i < futures.size(); ++i) {
            if (!collected[i] && futureIsReady(futures[i])) {
                collected[i] = true;
                --remaining;
                any_ready = true;
                on_ready(i);
            }
        }
        if (remaining > 0 && !any_ready) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    for (size_t i = 0; i < futures.size(); ++i) {
        if (!collected[i]) {
            if (futureIsReady(futures[i])) {
                on_ready(i);
            } else {
                on_timeout(i);
            }
        }
    }
}

}  // namespace

struct AsyncProducerCancelState {
    std::atomic<bool>                  cancelled{false};
    std::mutex                         mu;
    std::weak_ptr<grpc::ClientContext> client_context;
    std::weak_ptr<GenerateStream>      stream;
};

std::function<void()> makeAsyncProducerCancelCallback(const std::shared_ptr<AsyncProducerCancelState>& state) {
    return [state] {
        bool expected = false;
        if (!state->cancelled.compare_exchange_strong(expected, true)) {
            return;
        }

        std::shared_ptr<grpc::ClientContext> client_context;
        std::shared_ptr<GenerateStream>      stream;
        {
            std::lock_guard<std::mutex> lock(state->mu);
            client_context = state->client_context.lock();
            stream         = state->stream.lock();
        }
        if (client_context) {
            client_context->TryCancel();
        }
        if (stream) {
            stream->reportError(ErrorCode::CANCELLED, "request cancelled");
        }
    };
}

void refreshAsyncProducerCancelState(const std::shared_ptr<AsyncProducerCancelState>& state,
                                     const std::shared_ptr<grpc::ClientContext>&      client_context,
                                     const std::shared_ptr<GenerateStream>&           stream) {
    bool should_cancel = false;
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->client_context = client_context;
        state->stream         = stream;
        should_cancel         = state->cancelled.load();
    }
    if (should_cancel) {
        if (client_context) {
            client_context->TryCancel();
        }
        if (stream) {
            stream->reportError(ErrorCode::CANCELLED, "request cancelled");
        }
    }
}

class ScopeExit {
public:
    explicit ScopeExit(std::function<void()> fn): fn_(std::move(fn)) {}
    ~ScopeExit() {
        if (fn_) {
            fn_();
        }
    }
    ScopeExit(const ScopeExit&)            = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;

private:
    std::function<void()> fn_;
};

void cancelResponseEntry(const std::shared_ptr<ResponseBufferEntry>& entry) {
    if (!entry) {
        return;
    }
    std::function<void()> cancel_producer;
    {
        std::lock_guard<std::mutex> lock(entry->mu);
        entry->cancelled.store(true);
        entry->last_activity_us = currentTimeUs();
        cancel_producer         = entry->cancel_producer;
        entry->cancel_producer  = nullptr;
    }
    if (cancel_producer) {
        cancel_producer();
    }
    entry->cv.notify_all();
}

namespace {

const char* prefillStageName(PrefillStatInfo::ExecuteStage stage) {
    switch (stage) {
        case PrefillStatInfo::start:
            return "start";
        case PrefillStatInfo::getRpcConnection:
            return "getRpcConnection";
        case PrefillStatInfo::multimodalProcess:
            return "multimodalProcess";
        case PrefillStatInfo::remoteAllocateResource:
            return "remoteAllocateResource";
        case PrefillStatInfo::enqueueRequest:
            return "enqueueRequest";
        case PrefillStatInfo::remoteLoadCacheStart:
            return "remoteLoadCacheStart";
        case PrefillStatInfo::pollLocalOutput:
            return "pollLocalOutput";
        case PrefillStatInfo::remoteLoadCacheEnd:
            return "remoteLoadCacheEnd";
        case PrefillStatInfo::RemoteGenerate:
            return "RemoteGenerate";
        case PrefillStatInfo::pollRemoteOutput:
            return "pollRemoteOutput";
        case PrefillStatInfo::finish:
            return "finish";
        default:
            return "unknown";
    }
}

void logPrefillFailureTrace(const char* event, PrefillGenerateContext& prefill_context) {
    if (!prefillTraceLogEnabled()) {
        return;
    }
    RTP_LLM_LOG_WARNING("Prefill request trace: event=%s request_id=%ld request_key=%s stage=%s retry_times=%ld "
                        "retry_cost_time_ms=%ld execute_time_ms=%ld decode_addr=%s grpc_code=%d grpc_message=%s "
                        "error_code=%d error_message=%s",
                        event,
                        prefill_context.request_id,
                        prefill_context.request_key.c_str(),
                        prefillStageName(prefill_context.stat_info.stage),
                        prefill_context.retry_times,
                        prefill_context.retry_cost_time_ms,
                        prefill_context.executeTimeMs(),
                        prefill_context.decode_addr.c_str(),
                        static_cast<int>(prefill_context.error_status.error_code()),
                        prefill_context.error_status.error_message().c_str(),
                        static_cast<int>(prefill_context.error_info.code()),
                        prefill_context.error_info.ToString().c_str());
}

}  // namespace

#define CLIENT_GRPC_RET_IF_ERROR(prefill_context, state, error_code_value)                                             \
    if (!(state)) {                                                                                                    \
        auto   new_error_code = error_code_value;                                                                      \
        string new_error_msg  = "decode addr is " + prefill_context.decode_addr + ", ";                                \
        new_error_msg += "execute time is " + std::to_string(prefill_context.executeTimeMs()) + "ms, ";                \
        new_error_msg += "request timeout is " + std::to_string(prefill_context.request_timeout_ms) + "ms, ";          \
        new_error_msg += "rpc connection pointer is "                                                                  \
                         + std::to_string((int64_t)prefill_context.grpc_connection.channel.get()) + ", ";              \
        if (prefill_context.getStream()) {                                                                             \
            auto first_token_rt_ms = prefill_context.getStream()->getTimeInfo().first_token_rt_us / 1000;              \
            if (first_token_rt_ms) {                                                                                   \
                new_error_msg += "stream first token rt is " + std::to_string(first_token_rt_ms) + "ms, ";             \
            }                                                                                                          \
            auto wait_time_ms = prefill_context.getStream()->getTimeInfo().wait_time_us / 1000;                        \
            if (wait_time_ms) {                                                                                        \
                new_error_msg += "stream wait time is " + std::to_string(wait_time_ms) + "ms, ";                       \
            }                                                                                                          \
        }                                                                                                              \
        auto status = prefill_context.closeGrpcStream();                                                               \
        if (!status.ok()) {                                                                                            \
            const auto& error_msg = status.error_message();                                                            \
            if (error_msg.find("Connect Failed") != std::string::npos) {                                               \
                new_error_code = ErrorCode::CONNECT_FAILED;                                                            \
                prefill_context.closeGrpcConnection();                                                                 \
            } else if (error_msg.find("No route to host") != std::string::npos) {                                      \
                new_error_code = ErrorCode::CONNECT_FAILED;                                                            \
                prefill_context.closeGrpcConnection();                                                                 \
            } else if (error_msg.find("Connection reset by peer") != std::string::npos) {                              \
                new_error_code = ErrorCode::CONNECTION_RESET_BY_PEER;                                                  \
                prefill_context.closeGrpcConnection();                                                                 \
            } else if (error_msg.find("Connection timed out") != std::string::npos) {                                  \
                new_error_code = ErrorCode::CONNECT_TIMEOUT;                                                           \
                prefill_context.closeGrpcConnection();                                                                 \
            } else if (error_msg.find("Deadline Exceeded") != std::string::npos) {                                     \
                new_error_code = ErrorCode::DEADLINE_EXCEEDED;                                                         \
                prefill_context.closeGrpcConnection();                                                                 \
            } else if (error_msg.find("keepalive watchdog timeout") != std::string::npos) {                            \
                new_error_code = ErrorCode::KEEP_ALIVE_TIMEOUT;                                                        \
                prefill_context.closeGrpcConnection();                                                                 \
            }                                                                                                          \
            new_error_msg += error_msg;                                                                                \
            if (status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED) {                                         \
                new_error_code = ErrorCode::DECODE_MALLOC_FAILED;                                                      \
            }                                                                                                          \
        } else {                                                                                                       \
            if (prefill_context.client_stream) {                                                                       \
                new_error_msg += "server disconnected with status::ok";                                                \
            }                                                                                                          \
        }                                                                                                              \
        if (prefill_context.getStream()) {                                                                             \
            prefill_context.getStream()->reportEvent(StreamEvents::Error, new_error_code, new_error_msg);              \
        }                                                                                                              \
        prefill_context.error_info = ErrorInfo(new_error_code, new_error_msg);                                         \
        prefill_context.error_status =                                                                                 \
            serializeErrorMsg(prefill_context.request_key, prefill_context.request_info, prefill_context.error_info);  \
        logPrefillFailureTrace("client_grpc_error", prefill_context);                                                  \
        return;                                                                                                        \
    }

void PrefillRpcServer::startResponseRegistryGc() {
    if (response_gc_thread_.joinable()) {
        return;
    }
    response_gc_stop_.store(false);
    response_gc_thread_ = std::thread([this] {
        std::unique_lock<std::mutex> lock(response_gc_mu_);
        int                          gc_counter = 0;
        while (!response_gc_stop_.load()) {
            response_gc_cv_.wait_for(lock, std::chrono::seconds(10), [this] { return response_gc_stop_.load(); });
            if (response_gc_stop_.load()) {
                break;
            }
            lock.unlock();
            reportPoolMetrics();
            gc_counter++;
            if (gc_counter >= 6) {  // GC every 60 seconds
                response_registry_.gc(std::chrono::minutes(10));
                gc_counter = 0;
            }
            lock.lock();
        }
    });
}

void PrefillRpcServer::stopResponseRegistryGc() {
    response_gc_stop_.store(true);
    response_gc_cv_.notify_all();
    if (response_gc_thread_.joinable()) {
        response_gc_thread_.join();
    }
}

bool PrefillRpcServer::tryStartAsyncResponseWorker() {
    std::lock_guard<std::mutex> lock(response_worker_mu_);
    if (response_worker_stop_.load()) {
        return false;
    }
    ++response_worker_count_;
    return true;
}

void PrefillRpcServer::finishAsyncResponseWorker() {
    {
        std::lock_guard<std::mutex> lock(response_worker_mu_);
        if (response_worker_count_ > 0) {
            --response_worker_count_;
        }
    }
    response_worker_cv_.notify_all();
}

void PrefillRpcServer::stopAsyncResponseWorkers() {
    {
        std::lock_guard<std::mutex> lock(response_worker_mu_);
        response_worker_stop_.store(true);
    }
    response_registry_.cancelAll();

    static constexpr auto        kStopTimeout = std::chrono::seconds(30);
    std::unique_lock<std::mutex> lock(response_worker_mu_);
    bool all_done = response_worker_cv_.wait_for(lock, kStopTimeout, [this] { return response_worker_count_ == 0; });

    if (!all_done) {
        RTP_LLM_LOG_WARNING("stopAsyncResponseWorkers: timeout after %lds, still %zu workers active. Force resetting.",
                            kStopTimeout.count(),
                            response_worker_count_);
        response_worker_count_ = 0;
        // Notify other waiters that we've force-reset
        response_worker_cv_.notify_all();
    }
}

std::string PrefillRpcServer::batchTargetAddrForDpRank(int dp_rank) const {
    if (dp_rank < 0 || dp_rank >= maga_init_params_.parallelism_config.dp_size) {
        return "";
    }
    const auto&   all_workers = maga_init_params_.runtime_config.all_worker_grpc_addrs;
    const int64_t tp_size     = std::max<int64_t>(1, maga_init_params_.parallelism_config.tp_size);
    const int64_t world_rank  = static_cast<int64_t>(dp_rank) * tp_size;
    if (world_rank >= 0 && world_rank < static_cast<int64_t>(all_workers.size())) {
        return all_workers[world_rank];
    }
    if (dp_rank == maga_init_params_.parallelism_config.dp_rank
        && !maga_init_params_.runtime_config.worker_grpc_addrs.empty()) {
        return maga_init_params_.runtime_config.worker_grpc_addrs.front();
    }
    return "";
}

grpc::Status PrefillRpcServer::init(const EngineInitParams&                                maga_init_params,
                                    py::object                                             mm_process_engine,
                                    std::unique_ptr<rtp_llm::ProposeModelEngineInitParams> propose_params) {
    RTP_LLM_CHECK_WITH_INFO(maga_init_params.pd_sep_config.role_type == RoleType::PREFILL,
                            "prefill's role_type must be PREFILL");
    auto ret = RemoteRpcServer::init(maga_init_params, mm_process_engine, std::move(propose_params));
    if (!ret.ok()) {
        return ret;
    }
    initThreadPools();
    startResponseRegistryGc();
    return grpc::Status::OK;
}

void PrefillRpcServer::initThreadPools() {
    const auto& parallelism_config = maga_init_params_.parallelism_config;
    const auto& scheduler_config   = maga_init_params_.runtime_config.fifo_scheduler_config;
    const int   dp_size            = std::max(1, static_cast<int>(parallelism_config.dp_size));
    const int   max_context_batch  = std::max(1, static_cast<int>(scheduler_config.max_context_batch_size));

    // enqueue pool: L1 DP dispatch only (fast, ms-level, must never block)
    // Pool size: max(4, dp_size × (dp_size ≤ 4 ? 4 : 2))
    const int enqueue_threads = std::max(4, dp_size * (dp_size <= 4 ? 4 : 2));
    const int enqueue_queue   = dp_size * 10;

    enqueue_worker_pool_ =
        std::make_shared<autil::LockFreeThreadPool>(enqueue_threads, enqueue_queue, nullptr, "PrefillEnqueuePool");
    RTP_LLM_CHECK_WITH_INFO(enqueue_worker_pool_->start(), "PrefillRpcServer enqueue thread pool start failed");
    RTP_LLM_LOG_INFO("PrefillRpcServer enqueue pool started: threads=%d queue=%d", enqueue_threads, enqueue_queue);

    // worker lambda pool: heavy EnqueueGroup coordination (I/O-bound, ~12s per batch)
    // Pool size: max(4, dp_size × max_context_batch × 4)
    const int worker_lambda_threads = std::max(4, dp_size * max_context_batch * 4);
    const int worker_lambda_queue   = worker_lambda_threads * 4;

    worker_lambda_pool_ = std::make_shared<autil::LockFreeThreadPool>(
        worker_lambda_threads, worker_lambda_queue, nullptr, "PrefillWorkerPool");
    RTP_LLM_CHECK_WITH_INFO(worker_lambda_pool_->start(), "PrefillRpcServer worker lambda pool start failed");
    RTP_LLM_LOG_INFO(
        "PrefillRpcServer worker lambda pool started: threads=%d queue=%d (dp_size=%d max_context_batch=%d)",
        worker_lambda_threads,
        worker_lambda_queue,
        dp_size,
        max_context_batch);

    // slot pool: L2 Prepare + L3 Load + L4 Finish
    // Pool size: max(16, min(max_context_batch_size × 16, 128))
    const int slot_threads = std::max(16, std::min(max_context_batch * 16, 128));
    const int slot_queue   = slot_threads * 8;

    slot_worker_pool_ =
        std::make_shared<autil::LockFreeThreadPool>(slot_threads, slot_queue, nullptr, "PrefillSlotPool");
    RTP_LLM_CHECK_WITH_INFO(slot_worker_pool_->start(), "PrefillRpcServer slot thread pool start failed");
    RTP_LLM_LOG_INFO("PrefillRpcServer slot pool started: threads=%d queue=%d (dp_size=%d max_context_batch=%d)",
                     slot_threads,
                     slot_queue,
                     dp_size,
                     max_context_batch);
}

void PrefillRpcServer::reportPoolMetrics() {
    // Periodically log pool health (called every 10s from GC thread)
    RTP_LLM_LOG_INFO("PoolMetrics enqueue: active=%zu queued=%zu completed=%zu rejected=%zu fallback=%zu",
                     enqueue_pool_metrics_.active.load(),
                     enqueue_pool_metrics_.queued.load(),
                     enqueue_pool_metrics_.completed.load(),
                     enqueue_pool_metrics_.rejected.load(),
                     enqueue_pool_metrics_.fallback.load());
    RTP_LLM_LOG_INFO("PoolMetrics worker_lambda: active=%zu queued=%zu completed=%zu rejected=%zu fallback=%zu",
                     worker_lambda_pool_metrics_.active.load(),
                     worker_lambda_pool_metrics_.queued.load(),
                     worker_lambda_pool_metrics_.completed.load(),
                     worker_lambda_pool_metrics_.rejected.load(),
                     worker_lambda_pool_metrics_.fallback.load());
    RTP_LLM_LOG_INFO(
        "PoolMetrics slot: active=%zu queued=%zu completed=%zu rejected=%zu fallback=%zu response_workers=%zu",
        slot_pool_metrics_.active.load(),
        slot_pool_metrics_.queued.load(),
        slot_pool_metrics_.completed.load(),
        slot_pool_metrics_.rejected.load(),
        slot_pool_metrics_.fallback.load(),
        response_worker_count_);
}

ErrorInfo PrefillRpcServer::waitStreamBeforeRun(std::shared_ptr<GenerateStream> stream) {
    static int max_wait_timeout_us = maga_init_params_.pd_sep_config.prefill_max_wait_timeout_ms * 1000;
    auto       begin_time_us       = currentTimeUs();
    while (!stream->hasError() && stream->getStatus() == StreamState::WAITING) {
        usleep(100);
        auto current_time_us = currentTimeUs();
        auto cost_time_us    = current_time_us - begin_time_us;
        if (cost_time_us > max_wait_timeout_us) {
            string new_error_msg = "wait to run timeout, timeout is " + std::to_string(max_wait_timeout_us) + " us";
            stream->reportEvent(StreamEvents::Error, ErrorCode::WAIT_TO_RUN_TIMEOUT, new_error_msg);
            return ErrorInfo(ErrorCode::WAIT_TO_RUN_TIMEOUT, new_error_msg);
        }
    }
    if (stream->hasError()) {
        return stream->statusInfo();
    }
    return ErrorInfo::OkStatus();
}

void PrefillRpcServer::getRpcConnection(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    RTP_LLM_LOG_DEBUG("request [%ld] trans query", prefill_context.request_id);
    auto input                   = QueryConverter::transQuery(prefill_context.rpc_context.request);
    prefill_context.request_info = input->request_info;
    if (applyTimelineGate(prefill_context.request_key,
                          input->generate_config->gen_timeline,
                          input->generate_config->profile_step,
                          input->generate_config->profile_trace_name)) {
        input->generate_config->gen_timeline = true;
    }
    input->generate_config->pd_separation = true;
    if (engine_->isMTPEagle()) {
        input->generate_config->force_disable_sp_run = false;
    } else {
        input->generate_config->force_disable_sp_run = true;
    }
    prefill_context.generate_input = input;

    RTP_LLM_LOG_DEBUG("request [%ld] get rpc connection", prefill_context.request_id);

    auto&                       role_addrs = prefill_context.generate_input->generate_config->role_addrs;
    std::shared_ptr<const Host> host;

    // Check if request specifies host for DECODE role
    for (auto& role_addr : role_addrs) {
        if (role_addr.role == RoleType::DECODE) {
            host = std::make_shared<const Host>(role_addr.ip, role_addr.grpc_port, role_addr.http_port);
            break;
        }
    }

    // If no host specified in request, check if there's a master role
    char* remote_rpc_server_ip_env = std::getenv("REMOTE_RPC_SERVER_IP");
    bool  has_master_role          = (remote_rpc_server_ip_env != nullptr && strlen(remote_rpc_server_ip_env) > 0);

    // If no host specified in request and no master role, this is a direct prefill request
    // In this case, we still need to select decode machines as specified in the requirements
    if (!host && !has_master_role) {
        // For direct prefill requests without master role, we still need to select decode machines
        // The current logic will fail as expected since no host is available
        RTP_LLM_LOG_DEBUG(
            "request [%ld] no host specified in request and no master role, need to select decode machines",
            prefill_context.request_id);
    }

    if (!host || host->ip.empty()) {
        prefill_context.error_info =
            ErrorInfo(ErrorCode::GET_HOST_FAILED, "get host for decode cluster " + decode_cluster_name_ + " failed");
        prefill_context.error_status =
            serializeErrorMsg(prefill_context.request_key, prefill_context.request_info, prefill_context.error_info);
        logPrefillFailureTrace("get_rpc_connection_no_decode_host", prefill_context);
        return;
    }
    auto decode_addr    = host->ip + ":" + std::to_string(host->rpc_port);
    auto connect_status = resource_.rpc_pool.getConnection(decode_addr);
    if (!connect_status.ok()) {
        prefill_context.error_info = ErrorInfo(ErrorCode::GET_CONNECTION_FAILED,
                                               "get grpc connection for decode addr " + decode_addr + " failed");
        prefill_context.error_status =
            serializeErrorMsg(prefill_context.request_key, prefill_context.request_info, prefill_context.error_info);
        prefill_context.decode_addr = decode_addr;
        logPrefillFailureTrace("get_rpc_connection_failed", prefill_context);
        return;
    }
    prefill_context.decode_addr     = decode_addr;
    prefill_context.grpc_connection = connect_status.value();

    RTP_LLM_LOG_DEBUG("request [%ld] get rpc connection done", prefill_context.request_id);
}

void PrefillRpcServer::multimodalProcess(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    auto& input = prefill_context.generate_input;
    if (mm_processor_ != nullptr && input->multimodal_inputs) {
        auto result = mm_processor_->updateMultimodalFeatures(input);
        CLIENT_GRPC_RET_IF_ERROR(prefill_context, result.ok(), result.code());

        auto mutable_request = const_cast<GenerateInputPB*>(prefill_context.rpc_context.request);
        mutable_request->clear_token_ids();
        // TODO(xinfei.sxf) optimize copy
        auto* ids_ptr = input->input_ids.data_ptr<int32_t>();
        for (size_t i = 0; i < input->input_ids.numel(); i++) {
            mutable_request->add_token_ids(ids_ptr[i]);
        }
    }
    if (prefill_context.hasError()) {
        logPrefillFailureTrace("multimodal_process_failed", prefill_context);
    }
}

void PrefillRpcServer::remoteAllocateResource(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    RTP_LLM_LOG_DEBUG("request [%ld] start to remote allocate resource", prefill_context.request_id);
    prefill_context.client_context.reset(new ClientContext());
    auto    request_timeout_ms = prefill_context.request_timeout_ms;
    auto    max_rpc_timeout_ms = maga_init_params_.pd_sep_config.max_rpc_timeout_ms;
    int64_t final_timeout_ms   = request_timeout_ms > 0 ? request_timeout_ms : max_rpc_timeout_ms;
    if (final_timeout_ms > 0) {
        auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(final_timeout_ms);
        prefill_context.client_context->set_deadline(deadline);
    }
    if (prefill_context.cancel_state) {
        refreshAsyncProducerCancelState(
            prefill_context.cancel_state, prefill_context.client_context, prefill_context.getStream());
    }
    // final_timeout_ms <= 0: skip set_deadline; gRPC treats it as no deadline.
    prefill_context.client_stream =
        std::move(prefill_context.grpc_connection.stub->RemoteGenerate(prefill_context.client_context.get()));
    auto&             client_stream = prefill_context.client_stream;
    GenerateRequestPB alloc_request;
    alloc_request.set_stage(RemoteStage::ALLOCATE);
    alloc_request.set_client_id(process_id_);
    alloc_request.set_request_id(prefill_context.request_id);
    // TODO(xinfei.sxf) reduce copy
    GenerateInputPB* new_request = new GenerateInputPB(*prefill_context.rpc_context.request);
    new_request->clear_group_size();
    new_request->clear_group_id();
    new_request->mutable_generate_config()->clear_group_timeout();
    alloc_request.set_allocated_input(new_request);
    for (auto& addrs : prefill_context.prefill_worker_cache_store_addrs) {
        alloc_request.add_peer_addrs(addrs);
    }

    // Propagate CP size so decode knows prefill used context-parallel page-RR.
    const auto& cp_cfg = maga_init_params_.parallelism_config.prefill_cp_config;
    if (cp_cfg.kv_cache_sharded && maga_init_params_.parallelism_config.tp_size > 1) {
        alloc_request.set_prefill_cp_size(static_cast<int32_t>(maga_init_params_.parallelism_config.tp_size));
    }

    CLIENT_GRPC_RET_IF_ERROR(
        prefill_context, client_stream->Write(alloc_request), ErrorCode::REMOTE_ALLOCATE_RESOURCE_WRITE_FAILED);
    GenerateOutputsPB allocate_response;
    CLIENT_GRPC_RET_IF_ERROR(
        prefill_context, client_stream->Read(&allocate_response), ErrorCode::REMOTE_ALLOCATE_RESOURCE_READ_FAILED);
    if (prefillTraceLogEnabled() && allocate_response.has_error_info()
        && allocate_response.error_info().error_code() != 0) {
        RTP_LLM_LOG_WARNING("Prefill request trace: event=remote_allocate_response_error request_id=%ld "
                            "decode_addr=%s remote_error_code=%d remote_error_message=%s",
                            prefill_context.request_id,
                            prefill_context.decode_addr.c_str(),
                            allocate_response.error_info().error_code(),
                            allocate_response.error_info().error_message().c_str());
    }
    RTP_LLM_LOG_DEBUG("request [%ld] remote allocate resource done", prefill_context.request_id);
}

void PrefillRpcServer::enqueueRequest(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    RTP_LLM_LOG_DEBUG("request [%ld] trans query", prefill_context.request_id);
    RTP_LLM_LOG_DEBUG("request [%ld] trans to stream success", prefill_context.request_id);
    auto stream = engine_->enqueue(prefill_context.generate_input);
    prefill_context.setStream(stream);
    if (prefill_context.cancel_state) {
        refreshAsyncProducerCancelState(
            prefill_context.cancel_state, prefill_context.client_context, prefill_context.getStream());
    }
    RTP_LLM_LOG_DEBUG("request [%ld] enqueue success", prefill_context.request_id);
}

void PrefillRpcServer::remoteLoadCacheStart(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    RTP_LLM_LOG_DEBUG("request [%ld] remote load cache", prefill_context.request_id);
    prefill_context.error_info = waitStreamBeforeRun(prefill_context.getStream());
    if (prefill_context.error_info.hasError()) {
        prefill_context.error_status =
            serializeErrorMsg(prefill_context.request_key, prefill_context.request_info, prefill_context.error_info);
        logPrefillFailureTrace("wait_stream_before_run_failed", prefill_context);
        return;
    }
    AtomicGuard       request_guard(loading_cache_requests_);
    GenerateRequestPB load_request;
    load_request.set_client_id(process_id_);
    load_request.set_request_id(prefill_context.request_id);
    load_request.set_start_time(currentTimeUs());
    CLIENT_GRPC_RET_IF_ERROR(
        prefill_context, prefill_context.client_stream->Write(load_request), ErrorCode::REMOTE_LOAD_KV_CACHE_FAILED);
}

void PrefillRpcServer::pollLocalOutput(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    RTP_LLM_LOG_DEBUG("request [%ld] start to poll local output", prefill_context.request_id);
    auto first_status = pollStreamOutput(prefill_context.server_context,
                                         prefill_context.request_key,
                                         prefill_context.rpc_context.writer,
                                         prefill_context.getStream());
    if (!first_status.ok()) {
        prefill_context.error_status = first_status;
        logPrefillFailureTrace("poll_local_output_failed", prefill_context);
        return;
    }
    RTP_LLM_LOG_DEBUG("request [%ld] poll local output end", prefill_context.request_id);

    auto stream = prefill_context.getStream();
    if (stream->hasError()) {
        prefill_context.finished     = true;
        prefill_context.error_status = grpc::Status(grpc::StatusCode::INTERNAL, stream->statusInfo().ToString());
        logPrefillFailureTrace("local_stream_failed", prefill_context);
    }
}

void PrefillRpcServer::remoteLoadCacheEnd(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    GenerateOutputsPB load_response;
    CLIENT_GRPC_RET_IF_ERROR(
        prefill_context, prefill_context.client_stream->Read(&load_response), ErrorCode::REMOTE_LOAD_KV_CACHE_FAILED);
    auto error_code = transRPCErrorCode(load_response.error_info().error_code());

    // Decode has finished loading cache, now safe to release KV cache blocks.
    // This is called after cache store transfer is complete.
    if (prefill_context.generate_input->generate_config->pd_separation) {
        prefill_context.getStream()->releaseKVCacheForPDSep();
    }

    CLIENT_GRPC_RET_IF_ERROR(prefill_context, error_code == ErrorCode::NONE_ERROR, error_code);
    RTP_LLM_LOG_DEBUG("request [%ld] remote load cache done", prefill_context.request_id);

    meta_->dequeue(prefill_context.request_id, prefill_context.getStream());
    if (!prefill_context.getStream()->hasEvent(StreamEvents::NeedRemoteGenerate)) {
        RTP_LLM_LOG_DEBUG("request [%ld] pd-sep prefill finished locally without remote generate, "
                          "skipping remote generate stages",
                          prefill_context.request_id);
        // Exit here to keep the remote load-cache completion and release ordering intact.
        prefill_context.finished = true;
    }
}

void PrefillRpcServer::remoteGenerate(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    RTP_LLM_LOG_DEBUG("request [%ld] start to remote generate", prefill_context.request_id);
    std::shared_ptr<GenerateStream> stream = prefill_context.getStream();
    RTP_LLM_LOG_DEBUG("remote generate stream[%s]: %s", stream->streamLogTag().c_str(), stream->debugString().c_str());
    vector<int> all_token   = stream->currentExecuteTokens();
    int         first_token = all_token[all_token.size() - 1];
    RTP_LLM_LOG_DEBUG("first token token id %d", first_token);
    GenerateRequestPB generate_request;
    generate_request.set_client_id(process_id_);
    generate_request.set_request_id(prefill_context.request_id);
    generate_request.set_first_generate_token_id(first_token);
    auto context_position_ids = stream->getContextPositionIds();
    if (context_position_ids.defined()) {
        generate_request.mutable_position_ids()->CopyFrom(
            {context_position_ids.data_ptr<int32_t>(),
             context_position_ids.data_ptr<int32_t>() + context_position_ids.numel()});
    }
    if (engine_->isMTPEagle()) {
        RTP_LLM_CHECK_WITH_INFO(stream->getProposeToken().size() > 0,
                                "mtp remote generate propose token should not be empty");
    }
    generate_request.mutable_propose_token_ids()->CopyFrom(
        {stream->getProposeToken().begin(), stream->getProposeToken().end()});

    auto sp_output_buffer = stream->getSPOutputBuffer();

    if (sp_output_buffer) {
        auto all_probs_cpu =
            sp_output_buffer->all_probs.is_cuda() ? sp_output_buffer->all_probs.cpu() : sp_output_buffer->all_probs;
        torch::Tensor hidden_states_cpu;
        if (!sp_output_buffer->hidden_states.defined()) {
            // dummy hidden states, so datatype is not important
            hidden_states_cpu = torch::empty({0}, torch::TensorOptions().dtype(torch::kFloat16));
        } else {
            hidden_states_cpu = sp_output_buffer->hidden_states.is_cuda() ? sp_output_buffer->hidden_states.cpu() :
                                                                            sp_output_buffer->hidden_states;
        }
        QueryConverter::transTensorPB(generate_request.mutable_propose_probs(), all_probs_cpu);
        QueryConverter::transTensorPB(generate_request.mutable_propose_hidden(), hidden_states_cpu);
    }

    generate_request.set_stage(RemoteStage::GENERATE);

    CLIENT_GRPC_RET_IF_ERROR(
        prefill_context, prefill_context.client_stream->Write(generate_request), ErrorCode::REMOTE_GENERATE_FAILED);
}

void PrefillRpcServer::pollRemoteOutput(PrefillGenerateContext& prefill_context) {
    RTP_LLM_PROFILE_FUNCTION();
    RTP_LLM_LOG_DEBUG("request [%ld] start to poll remote output", prefill_context.request_id);
    auto&             request_id = prefill_context.request_id;
    GenerateOutputsPB response;
    auto              prefill_total_reuse_len  = prefill_context.getStream()->initialReuseLength();
    auto              prefill_local_reuse_len  = prefill_context.getStream()->localReuseLength();
    auto              prefill_remote_reuse_len = prefill_context.getStream()->remoteReuseLength();
    auto              prefill_memory_reuse_len = prefill_context.getStream()->memoryReuseLength();

    auto first_token_rt_us = prefill_context.getStream()->getTimeInfo().first_token_rt_us;
    while (prefill_context.client_stream->Read(&response)) {
        if (prefill_context.server_context && prefill_context.server_context->IsCancelled()) {
            RTP_LLM_LOG_WARNING("request [%ld] cancel by user", request_id);
            prefill_context.error_status = grpc::Status(grpc::StatusCode::CANCELLED, "request cancelled");
            return;
        }
        if (response.flatten_output().aux_info_size() == 0) {
            RTP_LLM_LOG_ERROR("request [%ld] generate output size is 0", request_id);
            break;
        }
        for (size_t i = 0; i < response.flatten_output().aux_info_size(); i++) {
            response.mutable_flatten_output()->mutable_aux_info(i)->set_pd_sep(true);
        }
        int64_t cost_time_us = currentTimeUs() - prefill_context.request_begin_time_us;
        for (size_t i = 0; i < response.flatten_output().aux_info_size(); i++) {
            auto decode_total_reuse_len  = response.flatten_output().aux_info(i).total_reuse_len();
            auto decode_local_reuse_len  = response.flatten_output().aux_info(i).local_reuse_len();
            auto decode_remote_reuse_len = response.flatten_output().aux_info(i).remote_reuse_len();
            auto decode_memory_reuse_len = response.flatten_output().aux_info(i).memory_reuse_len();

            response.mutable_flatten_output()->mutable_aux_info(i)->set_first_token_cost_time_us(first_token_rt_us);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_cost_time_us(cost_time_us);

            response.mutable_flatten_output()->mutable_aux_info(i)->set_total_reuse_len(prefill_total_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_local_reuse_len(prefill_local_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_remote_reuse_len(prefill_remote_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_memory_reuse_len(prefill_memory_reuse_len);

            response.mutable_flatten_output()->mutable_aux_info(i)->set_prefill_total_reuse_len(
                prefill_total_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_prefill_local_reuse_len(
                prefill_local_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_prefill_remote_reuse_len(
                prefill_remote_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_prefill_memory_reuse_len(
                prefill_memory_reuse_len);

            response.mutable_flatten_output()->mutable_aux_info(i)->set_decode_total_reuse_len(decode_total_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_decode_local_reuse_len(decode_local_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_decode_remote_reuse_len(
                decode_remote_reuse_len);
            response.mutable_flatten_output()->mutable_aux_info(i)->set_decode_memory_reuse_len(
                decode_memory_reuse_len);
        }
        if (!prefill_context.rpc_context.writer->Write(response)) {
            RTP_LLM_LOG_WARNING("request [%ld] write outputs pb failed", request_id);
            prefill_context.error_status = grpc::Status(grpc::StatusCode::INTERNAL, "request write outputs pb failed");
            return;
        }
    }
    CLIENT_GRPC_RET_IF_ERROR(
        prefill_context, prefill_context.closeGrpcStream().ok(), ErrorCode::REMOTE_GENERATE_FAILED);
}

grpc::Status PrefillRpcServer::prepareAllocateResource(PrefillGenerateContext& prefill_context) {
    EXECUTE_STAGE_FUNC(getRpcConnection, prefill_context);
    EXECUTE_STAGE_FUNC(multimodalProcess, prefill_context);
    EXECUTE_STAGE_FUNC(remoteAllocateResource, prefill_context);
    return grpc::Status::OK;
}

}  // namespace rtp_llm
