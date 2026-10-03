#include "docker.hpp"
#include "engine.grpc.pb.h"
#include "file.hpp"
#include "lease_tracker.hpp"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <curl/curl.h>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <openssl/evp.h>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace pb = strata::v1;
using Clock = strata::LeaseTracker::Clock;
namespace {
volatile std::sig_atomic_t stopped = 0;
void on_signal(int) { stopped = 1; }
std::string env(const char *key, const std::string &fallback) {
    const char *value = std::getenv(key);
    return value ? value : fallback;
}
void log(const std::string &event, const std::string &detail = "",
         nlohmann::json fields = nlohmann::json::object()) {
    static std::mutex output_mutex;
    const auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm utc{};
    gmtime_r(&now, &utc);
    std::ostringstream timestamp;
    timestamp << std::put_time(&utc, "%FT%TZ");
    nlohmann::json payload{
        {"timestamp", timestamp.str()}, {"level", "INFO"}, {"event", event}, {"detail", detail}};
    payload.update(fields);
    std::lock_guard lock(output_mutex);
    std::cout << payload.dump() << std::endl;
}
class RPCError : public std::runtime_error {
  public:
    explicit RPCError(const grpc::Status &status)
        : std::runtime_error(status.error_message()), code(status.error_code()) {}
    grpc::StatusCode code;
};
struct Running {
    pb::Assignment assignment;
    std::string container;
    Clock::time_point started = Clock::now();
    strata::LeaseTracker lease;
    std::mutex mutex;
    bool fenced = false;
    bool terminating = false;
    bool executing = false;
    std::optional<pb::Outcome> outcome;
    int exit_code = 1;
    std::string reason;
    Running(pb::Assignment a, std::string id, Clock::time_point before)
        : assignment(std::move(a)), container(std::move(id)),
          lease(assignment.lease_seconds(), before) {}
};
class Agent {
  public:
    Agent()
        : docker(env("STRATA_DOCKER_SOCKET", "/var/run/docker.sock")),
          stub(pb::WorkerControl::NewStub(channel())),
          worker_id(env("STRATA_WORKER_ID", "worker-cpp-1")),
          token(env("STRATA_WORKER_TOKEN", "local-development-token")),
          cpu(std::stod(env("STRATA_WORKER_CPU", "2"))),
          memory(std::stol(env("STRATA_WORKER_MEMORY_MB", "1024"))) {
        auto images = nlohmann::json::parse(
            env("STRATA_ALLOWED_IMAGES",
                "[\"strata/python-workloads:local\",\"strata/wave-solver:local\"]"));
        for (const auto &image : images)
            allowed.insert(image.get<std::string>());
    }
    void run() {
        std::jthread watchdog([this](std::stop_token stop) {
            while (!stop.stop_requested()) {
                for (const auto &r : snapshot()) {
                    bool terminate = false;
                    bool executing = false;
                    {
                        std::lock_guard lock(r->mutex);
                        executing = r->executing;
                        if (!r->fenced && r->lease.expired()) {
                            r->fenced = true;
                            terminate = true;
                        }
                        if (!r->fenced && !r->outcome && r->executing &&
                            Clock::now() - r->started >=
                                std::chrono::seconds(r->assignment.timeout_seconds())) {
                            r->outcome = pb::TIMED_OUT;
                            r->exit_code = 137;
                            r->reason = "execution deadline";
                            r->terminating = true;
                            terminate = true;
                        }
                    }
                    if (terminate && executing) {
                        try {
                            docker.stop(r->container, grace);
                        } catch (const std::exception &e) {
                            log("stop_failed", e.what());
                            std::lock_guard lock(r->mutex);
                            r->fenced = true;
                        }
                        std::lock_guard lock(r->mutex);
                        r->terminating = false;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
        while (!stopped) {
            try {
                if (session.empty())
                    register_worker();
                tick();
            } catch (const RPCError &e) {
                log("rpc_failed", e.what());
                if (e.code == grpc::StatusCode::FAILED_PRECONDITION) {
                    cleanup_running();
                    session.clear();
                }
            } catch (const std::exception &e) {
                log("worker_tick_failed", e.what());
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        watchdog.request_stop();
        watchdog.join();
        cleanup_running();
    }

  private:
    strata::Docker docker;
    std::unique_ptr<pb::WorkerControl::Stub> stub;
    std::string worker_id, token, session;
    double cpu, interval = 5;
    long memory;
    std::atomic<int> grace{5};
    std::set<std::string> allowed;
    std::map<std::string, std::shared_ptr<Running>> running;
    std::mutex map_mutex;
    Clock::time_point next_heartbeat{};

    static std::shared_ptr<grpc::Channel> channel() {
        grpc::ChannelArguments args;
        args.SetMaxSendMessageSize(17 * 1024 * 1024);
        auto credentials = grpc::InsecureChannelCredentials();
        const auto ca = env("STRATA_RPC_CA", "");
        if (!ca.empty()) {
            std::ifstream file(ca);
            if (!file)
                throw std::runtime_error("cannot read RPC CA");
            grpc::SslCredentialsOptions tls;
            tls.pem_root_certs = std::string(std::istreambuf_iterator<char>(file), {});
            credentials = grpc::SslCredentials(tls);
        }
        return grpc::CreateCustomChannel(env("STRATA_RPC_TARGET", "rpc:50051"), credentials, args);
    }

    std::vector<std::shared_ptr<Running>> snapshot() {
        std::lock_guard lock(map_mutex);
        std::vector<std::shared_ptr<Running>> result;
        for (const auto &[id, r] : running) {
            (void)id;
            result.push_back(r);
        }
        return result;
    }
    template <class Reply, class Request, class Method>
    Reply call(Method method, const Request &request, const std::string &traceparent = "") {
        grpc::ClientContext context;
        context.AddMetadata("authorization", "Bearer " + token);
        if (!traceparent.empty())
            context.AddMetadata("traceparent", traceparent);
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
        Reply reply;
        const auto status = ((*stub).*method)(&context, request, &reply);
        if (!status.ok())
            throw RPCError(status);
        return reply;
    }
    pb::AttemptRequest credentials(const Running &r) {
        pb::AttemptRequest c;
        c.set_attempt_id(r.assignment.attempt_id());
        c.set_session_id(session);
        c.set_lease_token(r.assignment.lease_token());
        return c;
    }
    void register_worker() {
        pb::RegisterRequest request;
        request.set_worker_id(worker_id);
        request.set_cpu_total(cpu);
        request.set_memory_total_mb(memory);
        request.add_capabilities("python");
        request.add_capabilities("cpp");
        request.add_capabilities("dataset-inputs");
        request.add_capabilities("worker-cpp");
        request.add_capabilities("node:" + worker_id);
        const auto reply = call<pb::RegisterReply>(&pb::WorkerControl::Stub::Register, request);
        docker.cleanup(worker_id, reply.termination_grace_seconds());
        session = reply.session_id();
        interval = reply.heartbeat_interval();
        grace = reply.termination_grace_seconds();
        log("worker_registered", worker_id);
    }
    long available_memory() {
        std::ifstream info("/proc/meminfo");
        std::string key, units;
        long value;
        while (info >> key >> value >> units)
            if (key == "MemAvailable:")
                return std::min(memory, value / 1024);
        return memory;
    }
    void heartbeat() {
        const auto before = Clock::now();
        pb::HeartbeatRequest request;
        request.set_worker_id(worker_id);
        request.set_session_id(session);
        double load = 0;
        const bool measured = getloadavg(&load, 1) == 1;
        double own_cpu = 0;
        for (const auto &r : snapshot()) {
            std::lock_guard lock(r->mutex);
            if (!r->fenced) {
                auto *lease = request.add_leases();
                lease->set_attempt_id(r->assignment.attempt_id());
                lease->set_lease_token(r->assignment.lease_token());
                own_cpu += r->assignment.cpu();
            }
        }
        const double host_cpu = static_cast<double>(sysconf(_SC_NPROCESSORS_ONLN));
        const double outside_load = measured ? std::max(0.0, load - own_cpu) : 0;
        request.set_cpu_available(std::max(0.0, std::min(cpu, host_cpu - outside_load)));
        request.set_memory_available_mb(available_memory());
        const auto reply = call<pb::HeartbeatReply>(&pb::WorkerControl::Stub::Heartbeat, request);
        for (const auto &command : reply.commands()) {
            std::shared_ptr<Running> r;
            {
                std::lock_guard lock(map_mutex);
                auto it = running.find(command.attempt_id());
                if (it == running.end()) {
                    continue;
                }
                r = it->second;
            }
            bool should_stop = false;
            {
                std::lock_guard lock(r->mutex);
                if (command.valid() && !r->fenced)
                    r->lease.renew(command.lease_seconds(), before);
                else
                    r->fenced = true;
                if (command.cancel()) {
                    r->outcome = pb::CANCELLED;
                    r->exit_code = 137;
                    r->reason = "cancellation requested";
                    should_stop = r->executing;
                }
            }
            if (should_stop)
                docker.stop(r->container, grace);
        }
        next_heartbeat = before + std::chrono::duration_cast<Clock::duration>(
                                      std::chrono::duration<double>(interval));
    }
    void launch(const pb::Assignment &a, Clock::time_point before) {
        {
            std::lock_guard lock(map_mutex);
            if (running.contains(a.attempt_id()))
                return;
        }
        if (!allowed.contains(a.image())) {
            launch_failed(a, "image is not allowlisted on worker");
            return;
        }
        std::vector<std::string> command(a.command().begin(), a.command().end());
        std::string id;
        try {
            id = docker.create(a.attempt_id(), worker_id, a.image(), command, a.cpu(),
                               a.memory_mb(), a.has_inputs());
        } catch (const std::exception &e) {
            launch_failed(a, "container create failed");
            log("container_create_failed", e.what());
            return;
        }
        auto r = std::make_shared<Running>(a, id, before);
        {
            std::lock_guard lock(map_mutex);
            running.emplace(a.attempt_id(), r);
        }
        try {
            if (a.has_inputs())
                prepare_inputs(r);
            call<pb::Empty>(&pb::WorkerControl::Stub::Start, credentials(*r), a.traceparent());
            std::lock_guard lock(r->mutex);
            if (r->fenced || r->outcome || r->lease.expired())
                throw std::runtime_error("deadline before start");
            r->started = Clock::now();
            r->executing = true;
            docker.request("POST", "/containers/" + id + "/start");
        } catch (...) {
            {
                std::lock_guard lock(r->mutex);
                r->fenced = true;
            }
            launch_failed(a, "container launch failed or acknowledgement lost");
            return;
        }
        log("job_started", "",
            {{"job_id", a.job_id()}, {"attempt_id", a.attempt_id()}, {"worker_id", worker_id}});
    }
    void prepare_inputs(const std::shared_ptr<Running> &r) {
        const auto manifest =
            call<pb::InputManifestReply>(&pb::WorkerControl::Stub::InputManifest, credentials(*r));
        auto progress = [this, &r]() {
            {
                std::lock_guard lock(r->mutex);
                if (r->fenced || r->outcome || r->lease.expired())
                    throw std::runtime_error("input staging lost its lease");
            }
            if (Clock::now() >= next_heartbeat)
                heartbeat();
        };
        for (const auto &file : manifest.files()) {
            strata::File stream(std::tmpfile());
            if (!stream)
                throw std::runtime_error("cannot create input buffer");
            std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest(EVP_MD_CTX_new(),
                                                                           EVP_MD_CTX_free);
            EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr);
            long offset = 0;
            while (offset < file.size()) {
                progress();
                pb::ReadInputRequest request;
                *request.mutable_credentials() = credentials(*r);
                request.set_sha256(file.sha256());
                request.set_offset(offset);
                request.set_max_bytes(
                    static_cast<int>(std::min<long>(4 * 1024 * 1024, file.size() - offset)));
                grpc::ClientContext context;
                context.AddMetadata("authorization", "Bearer " + token);
                context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
                auto reader = stub->ReadInput(&context, request);
                pb::InputChunk chunk;
                long count = 0;
                while (reader->Read(&chunk)) {
                    if (std::fwrite(chunk.content().data(), 1, chunk.content().size(),
                                    stream.get()) != chunk.content().size())
                        throw std::runtime_error("cannot write input buffer");
                    EVP_DigestUpdate(digest.get(), chunk.content().data(), chunk.content().size());
                    count += chunk.content().size();
                }
                const auto status = reader->Finish();
                if (!status.ok())
                    throw RPCError(status);
                if (!count || offset + count > file.size())
                    throw std::runtime_error("input size mismatch");
                offset += count;
            }
            unsigned char hash[EVP_MAX_MD_SIZE];
            unsigned int length = 0;
            EVP_DigestFinal_ex(digest.get(), hash, &length);
            std::ostringstream hex;
            for (unsigned int i = 0; i < length; ++i)
                hex << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
            if (hex.str() != file.sha256())
                throw std::runtime_error("input SHA-256 mismatch");
            progress();
            docker.stage(r->assignment.attempt_id(), worker_id, r->assignment.image(), file.alias(),
                         file.name(), stream.get(), file.size(), progress);
        }
    }
    void launch_failed(const pb::Assignment &a, const std::string &reason) {
        pb::CompleteRequest failure;
        auto *c = failure.mutable_credentials();
        c->set_attempt_id(a.attempt_id());
        c->set_session_id(session);
        c->set_lease_token(a.lease_token());
        failure.set_outcome(pb::FAILED);
        failure.set_exit_code(-1);
        failure.set_reason(reason);
        try {
            call<pb::Empty>(&pb::WorkerControl::Stub::Complete, failure, a.traceparent());
        } catch (const RPCError &e) {
            log("launch_failure_report_unavailable", e.what());
        }
    }
    static std::string decode_logs(const std::string &raw) {
        std::string result;
        std::size_t offset = 0;
        while (offset + 8 <= raw.size()) {
            std::size_t size = 0;
            for (int i = 4; i < 8; ++i)
                size = (size << 8) | static_cast<unsigned char>(raw[offset + i]);
            if (size > raw.size() - offset - 8)
                break;
            result.append(raw, offset + 8, size);
            offset += 8 + size;
        }
        if (result.size() > 1024 * 1024)
            result.erase(0, result.size() - 1024 * 1024);
        return result;
    }
    void report(const std::shared_ptr<Running> &r) {
        pb::LogsRequest logs;
        *logs.mutable_credentials() = credentials(*r);
        logs.set_content(decode_logs(docker.request(
            "GET", "/containers/" + r->container + "/logs?stdout=true&stderr=true&tail=10000", "",
            2 * 1024 * 1024)));
        call<pb::Empty>(&pb::WorkerControl::Stub::PutLogs, logs, r->assignment.traceparent());
        for (const auto &[name, bytes] : docker.artifacts(r->container)) {
            if (Clock::now() >= next_heartbeat)
                heartbeat();
            {
                std::lock_guard lock(r->mutex);
                if (r->fenced)
                    return;
            }
            pb::ArtifactRequest artifact;
            *artifact.mutable_credentials() = credentials(*r);
            artifact.set_name(name);
            artifact.set_content_type("application/octet-stream");
            artifact.set_content(bytes);
            call<pb::ArtifactReply>(&pb::WorkerControl::Stub::PutArtifact, artifact,
                                    r->assignment.traceparent());
        }
        pb::CompleteRequest complete;
        *complete.mutable_credentials() = credentials(*r);
        {
            std::lock_guard lock(r->mutex);
            if (r->fenced || !r->outcome)
                return;
            complete.set_outcome(*r->outcome);
            complete.set_exit_code(r->exit_code);
            complete.set_reason(r->reason);
        }
        call<pb::Empty>(&pb::WorkerControl::Stub::Complete, complete, r->assignment.traceparent());
    }
    void tick() {
        if (Clock::now() >= next_heartbeat)
            heartbeat();
        for (const auto &r : snapshot()) {
            bool inspect;
            {
                std::lock_guard lock(r->mutex);
                inspect = !r->fenced && !r->outcome;
            }
            if (inspect) {
                const auto state =
                    nlohmann::json::parse(
                        docker.request("GET", "/containers/" + r->container + "/json"))
                        .at("State");
                if (!state.at("Running").get<bool>()) {
                    std::lock_guard lock(r->mutex);
                    if (!r->outcome) {
                        r->exit_code = state.at("ExitCode").get<int>();
                        r->outcome = r->exit_code == 0 ? pb::SUCCEEDED : pb::FAILED;
                        r->reason =
                            state.value("OOMKilled", false) ? "OOM killed" : "process exited";
                    }
                }
            }
            bool fenced, ready;
            {
                std::lock_guard lock(r->mutex);
                fenced = r->fenced;
                ready = r->outcome.has_value() && !r->terminating;
            }
            if (!fenced && !ready)
                continue;
            if (!fenced) {
                try {
                    report(r);
                } catch (const RPCError &e) {
                    if (e.code != grpc::StatusCode::FAILED_PRECONDITION)
                        throw;
                }
            }
            try {
                docker.remove(r->container, r->assignment.attempt_id());
            } catch (const std::exception &e) {
                log("container_cleanup_failed", e.what());
                continue;
            }
            std::lock_guard lock(map_mutex);
            running.erase(r->assignment.attempt_id());
        }
        pb::PollRequest request;
        request.set_worker_id(worker_id);
        request.set_session_id(session);
        const auto poll_started = Clock::now();
        const auto reply = call<pb::PollReply>(&pb::WorkerControl::Stub::Poll, request);
        for (const auto &a : reply.assignments())
            launch(a, poll_started);
    }
    void cleanup_running() {
        for (const auto &r : snapshot()) {
            {
                std::lock_guard lock(r->mutex);
                r->fenced = true;
            }
            try {
                docker.stop(r->container, grace);
                docker.remove(r->container, r->assignment.attempt_id());
            } catch (const std::exception &e) {
                log("cleanup_failed", e.what());
            }
        }
        std::lock_guard lock(map_mutex);
        running.clear();
    }
};
} // namespace
int main() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    try {
        Agent agent;
        agent.run();
    } catch (const std::exception &e) {
        log("worker_failed", e.what());
        curl_global_cleanup();
        return 1;
    }
    curl_global_cleanup();
    return 0;
}
