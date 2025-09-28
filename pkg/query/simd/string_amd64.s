#include "textflag.h"

// func compareBytes32AVX2(a, b []byte) bool
TEXT ·compareBytes32AVX2(SB), NOSPLIT, $0-49
    MOVQ    a_base+0(FP), SI
    MOVQ    a_len+8(FP), CX
    MOVQ    b_base+24(FP), DI

    CMPQ    CX, $32
    JG      cmp32_false

    CMPQ    CX, $16
    JG      cmp32_large

    CMPQ    CX, $8
    JG      cmp32_medium

    XORQ    BX, BX
cmp32_byte_loop:
    CMPQ    BX, CX
    JGE     cmp32_true
    MOVB    (SI)(BX*1), R8
    MOVB    (DI)(BX*1), R9
    CMPB    R8, R9
    JNE     cmp32_false
    INCQ    BX
    JMP     cmp32_byte_loop

cmp32_medium:
    MOVQ    (SI), R8
    MOVQ    (DI), R9
    CMPQ    R8, R9
    JNE     cmp32_false
    SUBQ    $8, CX
    ADDQ    $8, SI
    ADDQ    $8, DI
    JMP     cmp32_byte_loop

cmp32_large:
    VMOVDQU (SI), X0
    VMOVDQU (DI), X1
    VPCMPEQB    X0, X1, X2
    VPMOVMSKB   X2, AX
    CMPW    AX, $0xFFFF
    JNE     cmp32_false

    CMPQ    CX, $16
    JE      cmp32_true

    SUBQ    $16, CX
    ADDQ    $16, SI
    ADDQ    $16, DI
    JMP     cmp32_byte_loop

cmp32_true:
    MOVB    $1, ret+48(FP)
    RET

cmp32_false:
    MOVB    $0, ret+48(FP)
    RET

// func compareBytesLongAVX2(a, b []byte) bool
TEXT ·compareBytesLongAVX2(SB), NOSPLIT, $0-49
    MOVQ    a_base+0(FP), SI
    MOVQ    a_len+8(FP), CX
    MOVQ    b_base+24(FP), DI

    XORQ    BX, BX
    MOVQ    CX, R8
    SHRQ    $5, R8

cmplong_loop:
    CMPQ    R8, $0
    JE      cmplong_remainder

    VMOVDQU (SI)(BX*1), Y0
    VMOVDQU (DI)(BX*1), Y1
    VPCMPEQB    Y0, Y1, Y2
    VPMOVMSKB   Y2, AX
    CMPL    AX, $0xFFFFFFFF
    JNE     cmplong_false

    ADDQ    $32, BX
    DECQ    R8
    JMP     cmplong_loop

cmplong_remainder:
    CMPQ    BX, CX
    JGE     cmplong_true

    MOVB    (SI)(BX*1), R9
    MOVB    (DI)(BX*1), R10
    CMPB    R9, R10
    JNE     cmplong_false

    INCQ    BX
    JMP     cmplong_remainder

cmplong_true:
    MOVB    $1, ret+48(FP)
    VZEROUPPER
    RET

cmplong_false:
    MOVB    $0, ret+48(FP)
    VZEROUPPER
    RET