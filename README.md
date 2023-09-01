# CISO Library

This is a Rust ciso compression library. Reference compression and decompression tools are provided.

## Binaries

The library contains two binaries, `ciso` and `unciso` that compress and decompress provided images
respectively.

The compression tool splits images at about the 4GB boundary. The decompression tool supports
both split and non-split images. Passing an image with extension `.1.cso` will discover all other
parts in sequence.

## Library

### Compression and Decompression

The `ciso::write::write_ciso_data` function can be used to compress data. lz4-flex is used to compress blocks.
Currently, only a sector size of 2048 is supported.

The `ciso::read::CSOReader` struct can be used to read from compressed data.

### Split Files

The `ciso::split` module has wrappers for handling split files for both reading and writing. For a reference of how
to use them, see the provided binaries.
