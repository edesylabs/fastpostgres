#include "textflag.h"

// func sumInt64AVX512(data []int64) int64
TEXT ·sumInt64AVX512(SB), NOSPLIT, $0-32
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX

    VPXORQ  Z0, Z0, Z0
    XORQ    BX, BX
    MOVQ    CX, R8
    SHRQ    $3, R8

sum_loop:
    CMPQ    R8, $0
    JE      sum_remainder

    VMOVDQU64   (SI)(BX*8), Z1
    VPADDQ  Z1, Z0, Z0

    ADDQ    $8, BX
    DECQ    R8
    JMP     sum_loop

sum_remainder:
    VEXTRACTI64X4   $1, Z0, Y1
    VPADDQ  Y1, Y0, Y0
    VEXTRACTI128    $1, Y0, X1
    VPADDQ  X1, X0, X0
    VPSRLDQ $8, X0, X1
    VPADDQ  X1, X0, X0
    VMOVQ   X0, DX

sum_scalar_remainder:
    CMPQ    BX, CX
    JGE     sum_done
    ADDQ    (SI)(BX*8), DX
    INCQ    BX
    JMP     sum_scalar_remainder

sum_done:
    MOVQ    DX, ret+24(FP)
    VZEROUPPER
    RET

// func minInt64AVX512(data []int64) int64
TEXT ·minInt64AVX512(SB), NOSPLIT, $0-32
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX

    VMOVDQU64   (SI), Z0
    MOVQ        $8, BX
    MOVQ        CX, R8
    SHRQ        $3, R8
    DECQ        R8

min_loop:
    CMPQ    R8, $0
    JLE     min_remainder

    VMOVDQU64   (SI)(BX*8), Z1
    VPMINSQ Z1, Z0, Z0

    ADDQ    $8, BX
    DECQ    R8
    JMP     min_loop

min_remainder:
    VEXTRACTI64X4   $1, Z0, Y1
    VPMINSQ Y1, Y0, Y0
    VEXTRACTI128    $1, Y0, X1
    VPMINSQ X1, X0, X0
    VPSRLDQ $8, X0, X1
    VPMINSQ X1, X0, X0
    VMOVQ   X0, DX

min_scalar_remainder:
    CMPQ    BX, CX
    JGE     min_done
    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, DX
    CMOVQLT R9, DX
    INCQ    BX
    JMP     min_scalar_remainder

min_done:
    MOVQ    DX, ret+24(FP)
    VZEROUPPER
    RET

// func maxInt64AVX512(data []int64) int64
TEXT ·maxInt64AVX512(SB), NOSPLIT, $0-32
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX

    VMOVDQU64   (SI), Z0
    MOVQ        $8, BX
    MOVQ        CX, R8
    SHRQ        $3, R8
    DECQ        R8

max_loop:
    CMPQ    R8, $0
    JLE     max_remainder

    VMOVDQU64   (SI)(BX*8), Z1
    VPMAXSQ Z1, Z0, Z0

    ADDQ    $8, BX
    DECQ    R8
    JMP     max_loop

max_remainder:
    VEXTRACTI64X4   $1, Z0, Y1
    VPMAXSQ Y1, Y0, Y0
    VEXTRACTI128    $1, Y0, X1
    VPMAXSQ X1, X0, X0
    VPSRLDQ $8, X0, X1
    VPMAXSQ X1, X0, X0
    VMOVQ   X0, DX

max_scalar_remainder:
    CMPQ    BX, CX
    JGE     max_done
    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, DX
    CMOVQGT R9, DX
    INCQ    BX
    JMP     max_scalar_remainder

max_done:
    MOVQ    DX, ret+24(FP)
    VZEROUPPER
    RET

// func countInt64WithNullsAVX512(data []int64, nulls []bool) int64
TEXT ·countInt64WithNullsAVX512(SB), NOSPLIT, $0-56
    MOVQ    data_len+8(FP), CX
    MOVQ    nulls_base+24(FP), DI
    MOVQ    nulls_len+32(FP), R12

    CMPQ    R12, $0
    JE      count_no_nulls

    XORQ    DX, DX
    XORQ    BX, BX

count_loop:
    CMPQ    BX, CX
    JGE     count_done

    MOVB    (DI)(BX*1), R9
    CMPB    R9, $0
    JE      count_not_null

    INCQ    BX
    JMP     count_loop

count_not_null:
    INCQ    DX
    INCQ    BX
    JMP     count_loop

count_no_nulls:
    MOVQ    CX, DX

count_done:
    MOVQ    DX, ret+48(FP)
    RET