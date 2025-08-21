CREATE TYPE tensor;
CREATE TYPE tensor (
    INPUT = tensor_in,
    OUTPUT = tensor_out,
    TYPMODIN = tensor_typmod_in,
    TYPMODOUT = tensor_typmod_out,
    INTERNALLENGTH = -1
);