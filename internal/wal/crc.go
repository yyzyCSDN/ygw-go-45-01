package wal

import (
	"encoding/binary"
	"hash/crc32"
)

const crcSize = 4

// crcTable is the CRC-32 (Castagnoli) table used for record integrity checks.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// checksum computes the integrity checksum of a record payload.
func checksum(payload []byte) uint32 {
	return crc32.Checksum(payload, crcTable)
}

// verifyChecksumOf checks a fixed-length frame's payload against its trailing
// checksum slice. Both slices carry exactly recordSize / crcSize bytes in the
// framed on-disk layout.
func verifyChecksumOf(payload, sum []byte) bool {
	if len(payload) < recordSize || len(sum) < crcSize {
		return false
	}
	expected := binary.LittleEndian.Uint32(sum[:crcSize])
	return checksum(payload[:recordSize]) == expected
}
