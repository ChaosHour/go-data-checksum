package checksum

// This file ensures that ChecksumContext is properly exported and
// available to all packages that import it.

// Re-export ChecksumContext to avoid undefined type errors
var _ *ChecksumContext = nil
