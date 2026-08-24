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

// appendChecksum appends the checksum to a record payload.
func appendChecksum(payload []byte) []byte {
	out := make([]byte, len(payload)+crcSize)
	copy(out, payload)
	binary.LittleEndian.PutUint32(out[len(payload):], checksum(payload))
	return out
}

// verifyChecksum reports whether the trailing checksum of a record matches its
// payload.
func verifyChecksum(record []byte) bool {
	if len(record) < recordSize+crcSize {
		return false
	}
	payload := record[:recordSize]
	expected := binary.LittleEndian.Uint32(record[recordSize:])
	return checksum(payload) == expected
}
