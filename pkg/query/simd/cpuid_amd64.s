#include "textflag.h"

// func cpuidDetect() CPUFeatures
TEXT ·cpuidDetect(SB), NOSPLIT, $0-5
    // Check CPUID support
    PUSHFQ
    POPQ    AX
    MOVQ    AX, CX
    XORQ    $(1<<21), AX
    PUSHQ   AX
    POPFQ
    PUSHFQ
    POPQ    AX
    XORQ    CX, AX
    PUSHQ   CX
    POPFQ

    // Get max CPUID input value
    MOVL    $0, AX
    CPUID
    CMPL    AX, $7
    JL      no_avx512

    // Check AVX, AVX2
    MOVL    $1, AX
    CPUID

    // ECX bit 28: AVX
    MOVL    CX, BX
    ANDL    $(1<<28), BX
    SHRL    $28, BX
    MOVB    BX, ret+2(FP)   // HasAVX

    // ECX bit 20: SSE4.2
    MOVL    CX, BX
    ANDL    $(1<<20), BX
    SHRL    $20, BX
    MOVB    BX, ret+3(FP)   // HasSSE42

    // ECX bit 19: SSE4.1
    MOVL    CX, BX
    ANDL    $(1<<19), BX
    SHRL    $19, BX
    MOVB    BX, ret+4(FP)   // HasSSE41

    // Check AVX2
    MOVL    $7, AX
    MOVL    $0, CX
    CPUID

    // EBX bit 5: AVX2
    MOVL    BX, AX
    ANDL    $(1<<5), AX
    SHRL    $5, AX
    MOVB    AX, ret+1(FP)   // HasAVX2

    // EBX bit 16: AVX512F
    MOVL    BX, AX
    ANDL    $(1<<16), AX
    SHRL    $16, AX
    MOVB    AX, ret+0(FP)   // HasAVX512

    RET

no_avx512:
    XORB    AL, AL
    MOVB    AL, ret+0(FP)   // HasAVX512 = false
    MOVB    AL, ret+1(FP)   // HasAVX2 = false
    MOVB    AL, ret+2(FP)   // HasAVX = false
    MOVB    AL, ret+3(FP)   // HasSSE42 = false
    MOVB    AL, ret+4(FP)   // HasSSE41 = false
    RET