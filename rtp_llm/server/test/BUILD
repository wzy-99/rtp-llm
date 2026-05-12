load("@arch_config//:arch_select.bzl", get_triton_deps = "triton_deps", "internal_deps")

py_test(
    name = "schedule_meta_test",
    srcs = ["schedule_meta_test.py"],
    deps = [
        "//rtp_llm:testlib",
        "//rtp_llm/server:server",
        "//rtp_llm:config_ops",
        "//rtp_llm:pydantic",
    ],
    tags = ["manual"],
)

py_test(
    name = "host_service_test",
    srcs = ["host_service_test.py"],
    deps = [
        "//rtp_llm:testlib",
        "//rtp_llm/server:server",
        "//rtp_llm:config_ops",
        "//rtp_llm:pydantic",
    ],
    tags = ["manual"],
)

py_test(
    name = "request_headers_test",
    srcs = ["request_headers_test.py"],
    deps = [
        "//rtp_llm:testlib",
        "//rtp_llm/server:server",
    ],
    tags = ["frontend"],
)

py_test(
    name = "master_client_test",
    srcs = ["master_client_test.py"],
    deps = [
        "//rtp_llm:testlib",
        "//rtp_llm/server:server",
        "//rtp_llm:config_ops",
        "//rtp_llm:pydantic",
    ],
    tags = ["frontend"],
)
