#include "textflag.h"

// func scanInt64AVX512(data []int64, target int64, result []bool) int
TEXT ·scanInt64AVX512(SB), NOSPLIT, $0-52
    MOVQ    data_base+0(FP), SI     // SI = data pointer
    MOVQ    data_len+8(FP), CX      // CX = data length
    MOVQ    target+24(FP), AX       // AX = target value
    MOVQ    result_base+32(FP), DI  // DI = result pointer

    // Broadcast target to all 8 lanes of ZMM0
    VPBROADCASTQ    AX, Z0

    XORQ    DX, DX                  // DX = match count
    XORQ    BX, BX                  // BX = index

    // Calculate number of full 8-element chunks
    MOVQ    CX, R8
    SHRQ    $3, R8                  // R8 = length / 8

loop_avx512:
    CMPQ    R8, $0
    JE      remainder

    // Load 8 int64 values (512 bits) into ZMM1
    VMOVDQU64   (SI)(BX*8), Z1

    // Compare all 8 values with target
    VPCMPEQQ    Z0, Z1, K1          // K1 = mask of equal values

    // Convert mask to byte mask and store results
    KMOVB   K1, R9                  // R9 = 8-bit mask

    // Process each bit in the mask
    MOVQ    $8, R10
process_mask:
    MOVB    R9, R11
    ANDB    $1, R11                 // Extract lowest bit
    MOVB    R11, (DI)(BX*1)         // Store boolean result

    // Update match count
    MOVZBQX R11, R11
    ADDQ    R11, DX

    INCQ    BX                      // Increment index
    SHRB    $1, R9                  // Shift mask right
    DECQ    R10
    JNZ     process_mask

    DECQ    R8
    JMP     loop_avx512

remainder:
    // Handle remaining elements (< 8)
    CMPQ    BX, CX
    JGE     done

    MOVQ    (SI)(BX*8), R9          // Load one value
    CMPQ    R9, AX                  // Compare with target
    SETEQ   R10                     // R10 = 1 if equal, 0 otherwise
    MOVB    R10, (DI)(BX*1)         // Store result

    MOVZBQX R10, R10
    ADDQ    R10, DX                 // Update count

    INCQ    BX
    JMP     remainder

done:
    MOVQ    DX, ret+48(FP)          // Return match count
    VZEROUPPER                       // Clear upper bits of YMM/ZMM registers
    RET


// func scanInt64RangeAVX512(data []int64, min, max int64, result []bool) int
TEXT ·scanInt64RangeAVX512(SB), NOSPLIT, $0-60
    MOVQ    data_base+0(FP), SI     // SI = data pointer
    MOVQ    data_len+8(FP), CX      // CX = data length
    MOVQ    min+24(FP), AX          // AX = min value
    MOVQ    max+32(FP), R12         // R12 = max value
    MOVQ    result_base+40(FP), DI  // DI = result pointer

    // Broadcast min and max to all 8 lanes
    VPBROADCASTQ    AX, Z0          // Z0 = min repeated 8 times
    VPBROADCASTQ    R12, Z1         // Z1 = max repeated 8 times

    XORQ    DX, DX                  // DX = match count
    XORQ    BX, BX                  // BX = index

    // Calculate number of full 8-element chunks
    MOVQ    CX, R8
    SHRQ    $3, R8                  // R8 = length / 8

range_loop_avx512:
    CMPQ    R8, $0
    JE      range_remainder

    // Load 8 int64 values into ZMM2
    VMOVDQU64   (SI)(BX*8), Z2

    // Compare: data >= min
    VPCMPQ      $5, Z0, Z2, K1      // K1 = data >= min (comparison code 5 = GE)

    // Compare: data <= max
    VPCMPQ      $2, Z1, Z2, K2      // K2 = data <= max (comparison code 2 = LE)

    // Combine masks: K3 = K1 AND K2 (in range)
    KANDB       K1, K2, K3

    // Convert mask to byte mask
    KMOVB   K3, R9                  // R9 = 8-bit mask

    // Process each bit
    MOVQ    $8, R10
range_process_mask:
    MOVB    R9, R11
    ANDB    $1, R11
    MOVB    R11, (DI)(BX*1)

    MOVZBQX R11, R11
    ADDQ    R11, DX

    INCQ    BX
    SHRB    $1, R9
    DECQ    R10
    JNZ     range_process_mask

    DECQ    R8
    JMP     range_loop_avx512

range_remainder:
    CMPQ    BX, CX
    JGE     range_done

    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, AX                  // Compare with min
    JL      range_not_match
    CMPQ    R9, R12                 // Compare with max
    JG      range_not_match

    MOVB    $1, (DI)(BX*1)
    INCQ    DX
    JMP     range_next

range_not_match:
    MOVB    $0, (DI)(BX*1)

range_next:
    INCQ    BX
    JMP     range_remainder

range_done:
    MOVQ    DX, ret+56(FP)
    VZEROUPPER
    RET