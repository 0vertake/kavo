package object

import "hash/crc32"

// CombineCRC32C returns the Castagnoli checksum of concatenating a payload whose
// checksum is a with one whose checksum is b and whose length is n.
//
// Completing a multipart upload is the reason this exists: each part was hashed
// as it arrived, and the object's checksum is those hashes combined rather than
// a second pass over the bytes. n is the length of the second payload, not the
// first; a non-positive n leaves a unchanged.
func CombineCRC32C(a, b uint32, n int64) uint32 {
	if n <= 0 {
		return a
	}

	// zlib's crc32_combine, with Castagnoli's polynomial in place of IEEE. The
	// even/odd matrices are the linear operators that append 2^k zero bits to a
	// CRC, so applying them according to the bits of n is O(log n) in the
	// second payload's length rather than O(n) in its bytes.
	var even, odd [32]uint32
	odd[0] = crc32.Castagnoli
	row := uint32(1)
	for i := 1; i < 32; i++ {
		odd[i] = row
		row <<= 1
	}
	gf2Square(&even, &odd)
	gf2Square(&odd, &even)

	for {
		gf2Square(&even, &odd)
		if n&1 != 0 {
			a = gf2Times(&even, a)
		}
		n >>= 1
		if n == 0 {
			break
		}
		gf2Square(&odd, &even)
		if n&1 != 0 {
			a = gf2Times(&odd, a)
		}
		n >>= 1
		if n == 0 {
			break
		}
	}
	return a ^ b
}

func gf2Times(mat *[32]uint32, vec uint32) uint32 {
	var sum uint32
	for i := 0; vec != 0; i++ {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
		vec >>= 1
	}
	return sum
}

func gf2Square(square, mat *[32]uint32) {
	for i := range square {
		square[i] = gf2Times(mat, mat[i])
	}
}
