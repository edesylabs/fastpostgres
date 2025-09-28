#include "textflag.h"

// func filterInt64EqualAVX512(data []int64, target int64, selection []bool) int
TEXT ·filterInt64EqualAVX512(SB), NOSPLIT, $0-52
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX
    MOVQ    target+24(FP), AX
    MOVQ    selection_base+32(FP), DI

    VPBROADCASTQ    AX, Z0
    XORQ    DX, DX
    XORQ    BX, BX
    MOVQ    CX, R8
    SHRQ    $3, R8

eq_loop:
    CMPQ    R8, $0
    JE      eq_remainder
    VMOVDQU64   (SI)(BX*8), Z1
    VPCMPEQQ    Z0, Z1, K1
    KMOVB   K1, R9
    MOVQ    $8, R10
eq_mask:
    MOVB    R9, R11
    ANDB    $1, R11
    MOVB    R11, (DI)(BX*1)
    MOVZBQX R11, R11
    ADDQ    R11, DX
    INCQ    BX
    SHRB    $1, R9
    DECQ    R10
    JNZ     eq_mask
    DECQ    R8
    JMP     eq_loop

eq_remainder:
    CMPQ    BX, CX
    JGE     eq_done
    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, AX
    SETEQ   R10
    MOVB    R10, (DI)(BX*1)
    MOVZBQX R10, R10
    ADDQ    R10, DX
    INCQ    BX
    JMP     eq_remainder

eq_done:
    MOVQ    DX, ret+48(FP)
    VZEROUPPER
    RET

// func filterInt64NotEqualAVX512(data []int64, target int64, selection []bool) int
TEXT ·filterInt64NotEqualAVX512(SB), NOSPLIT, $0-52
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX
    MOVQ    target+24(FP), AX
    MOVQ    selection_base+32(FP), DI

    VPBROADCASTQ    AX, Z0
    XORQ    DX, DX
    XORQ    BX, BX
    MOVQ    CX, R8
    SHRQ    $3, R8

neq_loop:
    CMPQ    R8, $0
    JE      neq_remainder
    VMOVDQU64   (SI)(BX*8), Z1
    VPCMPEQQ    Z0, Z1, K1
    KNOTB   K1, K1
    KMOVB   K1, R9
    MOVQ    $8, R10
neq_mask:
    MOVB    R9, R11
    ANDB    $1, R11
    MOVB    R11, (DI)(BX*1)
    MOVZBQX R11, R11
    ADDQ    R11, DX
    INCQ    BX
    SHRB    $1, R9
    DECQ    R10
    JNZ     neq_mask
    DECQ    R8
    JMP     neq_loop

neq_remainder:
    CMPQ    BX, CX
    JGE     neq_done
    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, AX
    SETNE   R10
    MOVB    R10, (DI)(BX*1)
    MOVZBQX R10, R10
    ADDQ    R10, DX
    INCQ    BX
    JMP     neq_remainder

neq_done:
    MOVQ    DX, ret+48(FP)
    VZEROUPPER
    RET

// func filterInt64LessAVX512(data []int64, target int64, selection []bool) int
TEXT ·filterInt64LessAVX512(SB), NOSPLIT, $0-52
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX
    MOVQ    target+24(FP), AX
    MOVQ    selection_base+32(FP), DI

    VPBROADCASTQ    AX, Z0
    XORQ    DX, DX
    XORQ    BX, BX
    MOVQ    CX, R8
    SHRQ    $3, R8

lt_loop:
    CMPQ    R8, $0
    JE      lt_remainder
    VMOVDQU64   (SI)(BX*8), Z1
    VPCMPQ      $1, Z0, Z1, K1
    KMOVB   K1, R9
    MOVQ    $8, R10
lt_mask:
    MOVB    R9, R11
    ANDB    $1, R11
    MOVB    R11, (DI)(BX*1)
    MOVZBQX R11, R11
    ADDQ    R11, DX
    INCQ    BX
    SHRB    $1, R9
    DECQ    R10
    JNZ     lt_mask
    DECQ    R8
    JMP     lt_loop

lt_remainder:
    CMPQ    BX, CX
    JGE     lt_done
    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, AX
    SETLT   R10
    MOVB    R10, (DI)(BX*1)
    MOVZBQX R10, R10
    ADDQ    R10, DX
    INCQ    BX
    JMP     lt_remainder

lt_done:
    MOVQ    DX, ret+48(FP)
    VZEROUPPER
    RET

// func filterInt64LessEqualAVX512(data []int64, target int64, selection []bool) int
TEXT ·filterInt64LessEqualAVX512(SB), NOSPLIT, $0-52
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX
    MOVQ    target+24(FP), AX
    MOVQ    selection_base+32(FP), DI

    VPBROADCASTQ    AX, Z0
    XORQ    DX, DX
    XORQ    BX, BX
    MOVQ    CX, R8
    SHRQ    $3, R8

le_loop:
    CMPQ    R8, $0
    JE      le_remainder
    VMOVDQU64   (SI)(BX*8), Z1
    VPCMPQ      $2, Z0, Z1, K1
    KMOVB   K1, R9
    MOVQ    $8, R10
le_mask:
    MOVB    R9, R11
    ANDB    $1, R11
    MOVB    R11, (DI)(BX*1)
    MOVZBQX R11, R11
    ADDQ    R11, DX
    INCQ    BX
    SHRB    $1, R9
    DECQ    R10
    JNZ     le_mask
    DECQ    R8
    JMP     le_loop

le_remainder:
    CMPQ    BX, CX
    JGE     le_done
    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, AX
    SETLE   R10
    MOVB    R10, (DI)(BX*1)
    MOVZBQX R10, R10
    ADDQ    R10, DX
    INCQ    BX
    JMP     le_remainder

le_done:
    MOVQ    DX, ret+48(FP)
    VZEROUPPER
    RET

// func filterInt64GreaterAVX512(data []int64, target int64, selection []bool) int
TEXT ·filterInt64GreaterAVX512(SB), NOSPLIT, $0-52
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX
    MOVQ    target+24(FP), AX
    MOVQ    selection_base+32(FP), DI

    VPBROADCASTQ    AX, Z0
    XORQ    DX, DX
    XORQ    BX, BX
    MOVQ    CX, R8
    SHRQ    $3, R8

gt_loop:
    CMPQ    R8, $0
    JE      gt_remainder
    VMOVDQU64   (SI)(BX*8), Z1
    VPCMPQ      $6, Z0, Z1, K1
    KMOVB   K1, R9
    MOVQ    $8, R10
gt_mask:
    MOVB    R9, R11
    ANDB    $1, R11
    MOVB    R11, (DI)(BX*1)
    MOVZBQX R11, R11
    ADDQ    R11, DX
    INCQ    BX
    SHRB    $1, R9
    DECQ    R10
    JNZ     gt_mask
    DECQ    R8
    JMP     gt_loop

gt_remainder:
    CMPQ    BX, CX
    JGE     gt_done
    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, AX
    SETGT   R10
    MOVB    R10, (DI)(BX*1)
    MOVZBQX R10, R10
    ADDQ    R10, DX
    INCQ    BX
    JMP     gt_remainder

gt_done:
    MOVQ    DX, ret+48(FP)
    VZEROUPPER
    RET

// func filterInt64GreaterEqualAVX512(data []int64, target int64, selection []bool) int
TEXT ·filterInt64GreaterEqualAVX512(SB), NOSPLIT, $0-52
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX
    MOVQ    target+24(FP), AX
    MOVQ    selection_base+32(FP), DI

    VPBROADCASTQ    AX, Z0
    XORQ    DX, DX
    XORQ    BX, BX
    MOVQ    CX, R8
    SHRQ    $3, R8

ge_loop:
    CMPQ    R8, $0
    JE      ge_remainder
    VMOVDQU64   (SI)(BX*8), Z1
    VPCMPQ      $5, Z0, Z1, K1
    KMOVB   K1, R9
    MOVQ    $8, R10
ge_mask:
    MOVB    R9, R11
    ANDB    $1, R11
    MOVB    R11, (DI)(BX*1)
    MOVZBQX R11, R11
    ADDQ    R11, DX
    INCQ    BX
    SHRB    $1, R9
    DECQ    R10
    JNZ     ge_mask
    DECQ    R8
    JMP     ge_loop

ge_remainder:
    CMPQ    BX, CX
    JGE     ge_done
    MOVQ    (SI)(BX*8), R9
    CMPQ    R9, AX
    SETGE   R10
    MOVB    R10, (DI)(BX*1)
    MOVZBQX R10, R10
    ADDQ    R10, DX
    INCQ    BX
    JMP     ge_remainder

ge_done:
    MOVQ    DX, ret+48(FP)
    VZEROUPPER
    RET

// func filterInt64WithSelectionAVX512(data []int64, target int64, op CompareOp, selectionIn, selectionOut []bool) int
TEXT ·filterInt64WithSelectionAVX512(SB), NOSPLIT, $0-69
    MOVQ    data_base+0(FP), SI
    MOVQ    data_len+8(FP), CX
    MOVQ    target+24(FP), AX
    MOVQ    op+32(FP), R13
    MOVQ    selectionIn_base+40(FP), R14
    MOVQ    selectionOut_base+56(FP), DI

    VPBROADCASTQ    AX, Z0
    XORQ    DX, DX
    XORQ    BX, BX

sel_loop:
    CMPQ    BX, CX
    JGE     sel_done

    MOVB    (R14)(BX*1), R15
    CMPB    R15, $0
    JE      sel_skip

    MOVQ    (SI)(BX*8), R9
    XORQ    R10, R10

    CMPQ    R13, $0
    JNE     sel_check_ne
    CMPQ    R9, AX
    SETEQ   R10
    JMP     sel_store

sel_check_ne:
    CMPQ    R13, $1
    JNE     sel_check_lt
    CMPQ    R9, AX
    SETNE   R10
    JMP     sel_store

sel_check_lt:
    CMPQ    R13, $2
    JNE     sel_check_le
    CMPQ    R9, AX
    SETLT   R10
    JMP     sel_store

sel_check_le:
    CMPQ    R13, $3
    JNE     sel_check_gt
    CMPQ    R9, AX
    SETLE   R10
    JMP     sel_store

sel_check_gt:
    CMPQ    R13, $4
    JNE     sel_check_ge
    CMPQ    R9, AX
    SETGT   R10
    JMP     sel_store

sel_check_ge:
    CMPQ    R9, AX
    SETGE   R10

sel_store:
    MOVB    R10, (DI)(BX*1)
    MOVZBQX R10, R10
    ADDQ    R10, DX
    JMP     sel_next

sel_skip:
    MOVB    $0, (DI)(BX*1)

sel_next:
    INCQ    BX
    JMP     sel_loop

sel_done:
    MOVQ    DX, ret+64(FP)
    RET