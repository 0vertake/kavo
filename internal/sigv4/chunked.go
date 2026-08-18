package sigv4

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// aws-chunked framing. Each chunk is
//
//	<hex length>[;chunk-signature=<hex>]\r\n<length bytes>\r\n
//
// and the body ends with a zero-length chunk, optionally followed by trailing
// headers. The framing exists so a client can sign a body it has not finished
// producing: the alternative is buffering the whole object to hash it, which is
// exactly what neither side can afford.

// maxFramingLine bounds a framing line so a malformed body cannot make the
// server allocate. Real chunk headers are under 100 bytes.
const maxFramingLine = 1024

// chunkSigner carries the rolling state of a signed chunked body. Each chunk's
// signature includes the previous one, so a chain cannot be reordered, replayed
// or cut short without the next chunk failing.
type chunkSigner struct {
	key   []byte
	date  time.Time
	scope string
	prev  string
}

// expect returns the signature the next chunk must carry, given the hash of its
// data.
func (s *chunkSigner) expect(dataHash string) string {
	return sign(s.key, strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		s.date.UTC().Format("20060102T150405Z"),
		s.scope,
		s.prev,
		emptyHash,
		dataHash,
	}, "\n"))
}

// chunkedReader unwraps an aws-chunked body, verifying each chunk's signature as
// it goes when the client signed them.
//
// Verified chunk by chunk rather than at the end: a chunk's bytes are handed on
// only once its own signature has been checked, so a caller streaming a large
// object never holds unverified data and never has to undo work.
type chunkedReader struct {
	br     *bufio.Reader
	signer *chunkSigner // nil when the client sent an unsigned streaming payload
	closer io.Closer

	left     int64     // bytes still to come in the current chunk
	hash     hash.Hash // hash of the current chunk's data, nil when unsigned
	want     string    // signature the current chunk must produce
	done     bool
	err      error
	trailers http.Header
}

func newChunkedReader(body io.ReadCloser, signer *chunkSigner) io.ReadCloser {
	return &chunkedReader{br: bufio.NewReader(body), signer: signer, closer: body}
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	for c.left == 0 {
		if c.done {
			return 0, io.EOF
		}
		if c.err = c.next(); c.err != nil {
			return 0, c.err
		}
	}

	want := int64(len(p))
	if c.left < want {
		want = c.left
	}
	n, err := c.br.Read(p[:want])
	if c.hash != nil {
		c.hash.Write(p[:n])
	}
	c.left -= int64(n)
	if err != nil {
		// The framing declared bytes that never arrived: a body that stops
		// mid-chunk is truncated, not finished.
		c.err = fmt.Errorf("%w: chunk ended %d bytes early: %w", ErrPayload, c.left, err)
		return n, c.err
	}
	if c.left == 0 {
		if c.err = c.endOfChunk(); c.err != nil {
			return n, c.err
		}
	}
	return n, nil
}

// next reads the header of the following chunk and prepares to stream its data.
func (c *chunkedReader) next() error {
	// The CRLF that terminates the previous chunk's data arrives as an empty
	// line, and is the only blank line expected between chunks.
	header, err := c.line()
	for err == nil && header == "" {
		header, err = c.line()
	}
	if errors.Is(err, io.EOF) {
		// Accepting this would mean storing a body the client never finished
		// sending as if it were the whole object.
		return fmt.Errorf("%w: body ended before the zero-length chunk", ErrPayload)
	}
	if err != nil {
		return err
	}

	sizeHex, params, _ := strings.Cut(header, ";")
	size, err := strconv.ParseInt(strings.TrimSpace(sizeHex), 16, 64)
	if err != nil || size < 0 {
		return fmt.Errorf("%w: chunk size %q", ErrMalformed, sizeHex)
	}

	c.want = ""
	if c.signer != nil {
		sig, ok := strings.CutPrefix(strings.TrimSpace(params), "chunk-signature=")
		if !ok {
			return fmt.Errorf("%w: chunk without a signature in a signed body", ErrMalformed)
		}
		c.want = sig
		c.hash = sha256.New()
	}
	c.left = size

	if size == 0 {
		// The final chunk covers the empty string and is followed by whatever
		// trailers the client chose to send.
		c.done = true
		if err := c.endOfChunk(); err != nil {
			return err
		}
		return c.readTrailers()
	}
	return nil
}

// Trailers returns the headers that followed the zero-length chunk. Empty until
// the body has been read to EOF, and empty if this reader was not aws-chunked.
func Trailers(r io.ReadCloser) http.Header {
	type has interface{ Trailers() http.Header }
	if h, ok := r.(has); ok {
		return h.Trailers()
	}
	return nil
}

func (c *chunkedReader) Trailers() http.Header { return c.trailers }

// endOfChunk checks the chunk just read against its signature and links it into
// the chain the next chunk is checked against.
func (c *chunkedReader) endOfChunk() error {
	if c.signer == nil {
		return nil
	}
	got := c.signer.expect(hex.EncodeToString(c.hash.Sum(nil)))
	if !hmac.Equal([]byte(got), []byte(c.want)) {
		return fmt.Errorf("%w: chunk signature is %s, computed %s", ErrMismatch, c.want, got)
	}
	c.signer.prev = c.want
	return nil
}

// readTrailers consumes the trailing headers after the last chunk.
//
// A signed trailer's own signature is not checked: each chunk's signature
// already covered those bytes on the way in. The object checksum a client puts
// here is compared by the write, once it has hashed the body, against the same
// number — so the value is kept rather than discarded.
func (c *chunkedReader) readTrailers() error {
	c.trailers = make(http.Header)
	for {
		line, err := c.line()
		if errors.Is(err, io.EOF) || line == "" {
			return nil
		}
		if err != nil {
			return err
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return fmt.Errorf("%w: trailer line %q", ErrMalformed, line)
		}
		c.trailers.Add(strings.TrimSpace(name), strings.TrimSpace(value))
	}
}

// line reads one CRLF-terminated framing line.
func (c *chunkedReader) line() (string, error) {
	line, err := c.br.ReadString('\n')
	if len(line) > maxFramingLine {
		return "", fmt.Errorf("%w: chunk framing line of %d bytes", ErrMalformed, len(line))
	}
	switch {
	case errors.Is(err, io.EOF) && line == "":
		return "", io.EOF
	case err != nil && !errors.Is(err, io.EOF):
		return "", fmt.Errorf("%w: read chunk framing: %w", ErrPayload, err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (c *chunkedReader) Close() error { return c.closer.Close() }
