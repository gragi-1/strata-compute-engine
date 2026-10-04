"""A headless CUDA kernel with verified numerical output; no CPU fallback or toolkit dependency."""

import argparse
import ctypes as ct
import json
import time
from pathlib import Path

# Driver JIT compiles this small, fixed kernel. It squares one float per CUDA thread.
PTX = b"""
.version 7.0
.target sm_75
.address_size 64
.visible .entry square(.param .u64 input, .param .u64 output, .param .u32 count) {
    .reg .pred %p;
    .reg .b32 %r<6>;
    .reg .b64 %rd<6>;
    .reg .f32 %f<3>;
    ld.param.u64 %rd1, [input];
    ld.param.u64 %rd2, [output];
    ld.param.u32 %r1, [count];
    mov.u32 %r2, %ctaid.x;
    mov.u32 %r3, %ntid.x;
    mov.u32 %r4, %tid.x;
    mad.lo.s32 %r5, %r2, %r3, %r4;
    setp.ge.u32 %p, %r5, %r1;
    @%p bra done;
    mul.wide.u32 %rd3, %r5, 4;
    add.s64 %rd4, %rd1, %rd3;
    add.s64 %rd5, %rd2, %rd3;
    ld.global.f32 %f1, [%rd4];
    mul.f32 %f2, %f1, %f1;
    st.global.f32 [%rd5], %f2;
done:
    ret;
}
"""


def compute(count: int) -> dict:
    driver = ct.CDLL("libcuda.so.1")
    pointer, device_pointer = ct.c_void_p, ct.c_uint64
    signatures = {
        "cuInit": [ct.c_uint],
        "cuDeviceGet": [ct.POINTER(ct.c_int), ct.c_int],
        "cuDeviceGetName": [pointer, ct.c_int, ct.c_int],
        "cuCtxCreate_v2": [ct.POINTER(pointer), ct.c_uint, ct.c_int],
        "cuCtxDestroy_v2": [pointer],
        "cuModuleLoadData": [ct.POINTER(pointer), pointer],
        "cuModuleUnload": [pointer],
        "cuModuleGetFunction": [ct.POINTER(pointer), pointer, ct.c_char_p],
        "cuMemAlloc_v2": [ct.POINTER(device_pointer), ct.c_size_t],
        "cuMemFree_v2": [device_pointer],
        "cuMemcpyHtoD_v2": [device_pointer, pointer, ct.c_size_t],
        "cuMemcpyDtoH_v2": [pointer, device_pointer, ct.c_size_t],
        "cuCtxSynchronize": [],
        "cuLaunchKernel": [pointer, *([ct.c_uint] * 7), pointer, pointer, pointer],
    }
    for name, signature in signatures.items():
        function = getattr(driver, name)
        function.argtypes, function.restype = signature, ct.c_int

    def call(name, *args):
        status = getattr(driver, name)(*args)
        if status:
            raise RuntimeError(f"{name} failed with CUDA driver status {status}")

    device, context, module, function = ct.c_int(), pointer(), pointer(), pointer()
    buffers = []
    try:
        call("cuInit", 0)
        call("cuDeviceGet", ct.byref(device), 0)
        model = ct.create_string_buffer(256)
        call("cuDeviceGetName", model, len(model), device)
        call("cuCtxCreate_v2", ct.byref(context), 0, device)
        call("cuModuleLoadData", ct.byref(module), ct.cast(ct.c_char_p(PTX), pointer))
        call("cuModuleGetFunction", ct.byref(function), module, b"square")
        values = (ct.c_float * count)(*(index / count for index in range(count)))
        result = (ct.c_float * count)()
        for _ in range(2):
            allocation = device_pointer()
            call("cuMemAlloc_v2", ct.byref(allocation), ct.sizeof(values))
            buffers.append(allocation)
        call("cuMemcpyHtoD_v2", buffers[0], values, ct.sizeof(values))
        size = ct.c_uint(count)
        parameters = (pointer * 3)(
            ct.addressof(buffers[0]), ct.addressof(buffers[1]), ct.addressof(size)
        )
        call(
            "cuLaunchKernel",
            function,
            (count + 127) // 128,
            1,
            1,
            128,
            1,
            1,
            0,
            None,
            parameters,
            None,
        )
        call("cuCtxSynchronize")
        call("cuMemcpyDtoH_v2", result, buffers[1], ct.sizeof(result))
        error = max(abs(float(result[index]) - (index / count) ** 2) for index in range(count))
        if error > 1e-6:
            raise RuntimeError(f"CUDA output verification failed: maximum error {error}")
        return {
            "device": model.value.decode(),
            "values": count,
            "max_abs_error": error,
            "sum_squares": sum(result),
            "kernel": "square",
            "verified": True,
        }
    finally:
        for allocation in buffers:
            call("cuMemFree_v2", allocation)
        if module:
            call("cuModuleUnload", module)
        if context:
            call("cuCtxDestroy_v2", context)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "count", type=int, nargs="?", default=1024, choices=[1024, 2048, 4096, 8192, 16384]
    )
    parser.add_argument("hold", type=int, nargs="?", default=0, choices=range(6))
    args = parser.parse_args()
    result = compute(args.count)
    Path("/output/cuda-result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(f"CUDA verified: {args.count} values on {result['device']}", flush=True)
    time.sleep(args.hold)
