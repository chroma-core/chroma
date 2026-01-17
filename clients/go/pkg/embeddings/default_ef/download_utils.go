package defaultef

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
)

// Known SHA256 checksum for the ONNX model archive.
// This ensures the downloaded model has not been tampered with.
// To update: download the file and run `shasum -a 256 onnx.tar.gz`
const onnxModelSHA256 = "913d7300ceae3b2dbc2c50d1de4baacab4be7b9380491c27fab7418616a16ec3"

func verifyFileChecksum(filepath string, expectedChecksum string) error {
	if expectedChecksum == "" {
		return nil
	}

	file, err := os.Open(filepath)
	if err != nil {
		return errors.Wrapf(err, "failed to open file for checksum verification: %s", filepath)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return errors.Wrapf(err, "failed to compute checksum for: %s", filepath)
	}

	actualChecksum := hex.EncodeToString(hasher.Sum(nil))
	if actualChecksum != expectedChecksum {
		return errors.Errorf("checksum mismatch for %s: expected %s, got %s", filepath, expectedChecksum, actualChecksum)
	}

	return nil
}

func lockFile(path string) (*os.File, error) {
	lockPath := filepath.Join(path, ".lock")
	err := os.MkdirAll(filepath.Dir(lockPath), 0755)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create directory for lock file: %s", filepath.Dir(lockPath))
	}
	// Try to create lock file with exclusive access
	for i := 0; i < 30; i++ { // Wait up to 30 seconds

		lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err == nil {
			// Write PID to lock file for debugging
			fmt.Fprintf(lockFile, "%d", os.Getpid())
			_ = lockFile.Sync()
			return lockFile, nil
		}

		if !os.IsExist(err) {
			return nil, err
		}

		// Check if the process holding the lock is still alive
		if isLockStale(lockPath) {
			_ = os.Remove(lockPath) // Best-effort removal of stale lock, ignore errors (TOCTOU is acceptable)
			continue
		}

		time.Sleep(1 * time.Second)
	}

	return nil, errors.New("timeout waiting for file lock")
}

func unlockFile(lockFile *os.File) error {
	if lockFile == nil {
		return nil
	}
	lockPath := lockFile.Name()
	lockFile.Close()
	return os.Remove(lockPath)
}

func isLockStale(lockPath string) bool {
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return true // If we can't read it, assume stale
	}

	var pid int
	if _, err := fmt.Sscanf(string(data), "%d", &pid); err != nil {
		return true
	}

	// Check if process exists (Unix-specific)
	process, err := os.FindProcess(pid)
	if err != nil {
		return true
	}

	// Send signal 0 to check if process is alive
	err = process.Signal(syscall.Signal(0))
	return err != nil
}

func downloadFile(filepath string, url string) error {

	resp, err := http.Get(url)
	if err != nil {
		return errors.Wrap(err, "failed to make HTTP request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("unexpected response %s for URL %s", resp.Status, url)
	}

	// Check Content-Length if available
	contentLength := resp.ContentLength
	// if contentLength > 0 {
	//	fmt.Printf("Expected download size: %d bytes\n", contentLength)
	//}

	out, err := os.Create(filepath)
	if err != nil {
		return errors.Wrapf(err, "failed to create file: %s", filepath)
	}
	defer out.Close()

	// Copy directly from response body, don't buffer everything in memory
	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return errors.Wrapf(err, "failed to copy file contents: %s", filepath)
	}

	// fmt.Printf("Downloaded %d bytes\n", written)

	// Verify size if we know the expected size
	if contentLength > 0 && written != contentLength {
		return errors.Errorf("download incomplete: expected %d bytes, got %d bytes", contentLength, written)
	}

	// Explicitly sync to disk
	if err := out.Sync(); err != nil {
		return errors.Wrapf(err, "failed to sync file to disk: %s", filepath)
	}

	// Verify file exists and has expected size
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return errors.Wrapf(err, "failed to stat downloaded file: %s", filepath)
	}

	if fileInfo.Size() != written {
		return errors.Errorf("file size mismatch after download: expected %d, got %d", written, fileInfo.Size())
	}

	// fmt.Printf("Download completed and verified: %s (%d bytes)\n", filepath, fileInfo.Size())
	return nil
}

func verifyTarGzFile(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return errors.Wrapf(err, "could not open file for verification: %s", filepath)
	}
	defer file.Close()

	// Try to read the gzip header
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return errors.Wrap(err, "invalid gzip file")
	}
	defer gzipReader.Close()

	// Try to read the tar header
	tarReader := tar.NewReader(gzipReader)
	_, err = tarReader.Next()
	if err != nil {
		return errors.Wrap(err, "invalid tar file or corrupt archive")
	}

	return nil
}

func getOSAndArch() (string, string) {
	return runtime.GOOS, runtime.GOARCH
}

// safePath validates that joining destPath with filename results in a path
// within destPath, preventing path traversal attacks from malicious tar entries.
func safePath(destPath, filename string) (string, error) {
	destPath = filepath.Clean(destPath)
	targetPath := filepath.Join(destPath, filepath.Base(filename))
	if !strings.HasPrefix(targetPath, destPath+string(os.PathSeparator)) && targetPath != destPath {
		return "", errors.Errorf("invalid path: %q escapes destination directory", filename)
	}
	return targetPath, nil
}

func extractSpecificFile(tarGzPath, targetFile, destPath string) error {
	// Open the .tar.gz file
	f, err := os.Open(tarGzPath)
	if err != nil {
		return errors.Wrapf(err, "could not open tar.gz file: %s", tarGzPath)
	}
	defer f.Close()

	// Create a gzip reader
	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return errors.Wrap(err, "could not create gzip reader")
	}
	defer gzipReader.Close()

	// Create a tar reader
	tarReader := tar.NewReader(gzipReader)

	// Iterate through the files in the tar archive
	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break // End of archive
		}

		if err != nil {
			return errors.Wrap(err, "could not read tar header")
		}

		// Check if this is the file we're looking for
		if header.Name == targetFile {
			outPath, err := safePath(destPath, targetFile)
			if err != nil {
				return err
			}
			outFile, err := os.Create(outPath)
			if err != nil {
				return errors.Wrapf(err, "could not create output file: %s", outPath)
			}
			defer outFile.Close()
			if _, err := io.Copy(outFile, tarReader); err != nil {
				return errors.Wrapf(err, "could not copy file data to output file: %s", outPath)
			}
			if err := outFile.Sync(); err != nil {
				return errors.Wrapf(err, "could not sync output file to disk: %s", outPath)
			}
			return nil // Successfully extracted the file
		}
		if targetFile == "" {
			outPath, err := safePath(destPath, header.Name)
			if err != nil {
				return err
			}
			outFile, err := os.Create(outPath)
			if err != nil {
				return errors.Wrapf(err, "could not create output file: %s", outPath)
			}
			defer outFile.Close()
			if _, err := io.Copy(outFile, tarReader); err != nil {
				return errors.Wrap(err, "could not copy file data")
			}
			if err := outFile.Sync(); err != nil {
				return errors.Wrapf(err, "could not sync output file to disk: %s", outPath)
			}
		}
	}

	if targetFile != "" {
		expectedPath := filepath.Join(destPath, filepath.Base(targetFile))
		if _, err := os.Stat(expectedPath); err != nil {
			return errors.Wrapf(err, "extracted file not found at expected location: %s", expectedPath)
		}
	}
	return nil
}

var (
	onnxInitErr error
	onnxMu      sync.Mutex
)

func EnsureOnnxRuntimeSharedLibrary() error {
	cfg := getConfig()

	// If using custom path, just verify the file exists
	if cfg.LibOnnxRuntimeVersion == "custom" {
		if _, err := os.Stat(cfg.OnnxLibPath); err != nil {
			return errors.Wrapf(err, "custom ONNX Runtime library not found at: %s", cfg.OnnxLibPath)
		}
		return nil
	}

	onnxMu.Lock()
	defer onnxMu.Unlock()
	lockFile, err := lockFile(cfg.OnnxCacheDir)
	if err != nil {
		return errors.Wrap(err, "failed to acquire lock for onnx download")
	}
	defer func() {
		_ = unlockFile(lockFile)
	}()

	cos, carch := getOSAndArch()
	if carch == "amd64" {
		carch = "x64"
	}
	if cos == "darwin" {
		cos = "osx"
		if carch == "x64" {
			carch = "x86_64"
		}
	}

	downloadAndExtractNeeded := false
	if _, onnxInitErr = os.Stat(cfg.OnnxLibPath); os.IsNotExist(onnxInitErr) {
		downloadAndExtractNeeded = true
		onnxInitErr = os.MkdirAll(cfg.OnnxCacheDir, 0755)
		if onnxInitErr != nil {
			return errors.Wrap(onnxInitErr, "failed to create onnx cache")
		}
	}
	if !downloadAndExtractNeeded {
		return nil
	}
	targetArchive := filepath.Join(cfg.OnnxCacheDir, "onnxruntime-"+cos+"-"+carch+"-"+cfg.LibOnnxRuntimeVersion+".tgz")
	if _, onnxInitErr = os.Stat(cfg.OnnxLibPath); os.IsNotExist(onnxInitErr) {
		// Download the library from official Microsoft GitHub releases.
		// Note: Checksum verification is not practical here because versions are user-configurable
		// and each version/OS/arch combination has a unique checksum. Integrity is ensured through:
		// 1. HTTPS transport security 2. Archive format validation 3. File size verification
		url := "https://github.com/microsoft/onnxruntime/releases/download/v" + cfg.LibOnnxRuntimeVersion + "/onnxruntime-" + cos + "-" + carch + "-" + cfg.LibOnnxRuntimeVersion + ".tgz"
		if _, onnxInitErr = os.Stat(targetArchive); os.IsNotExist(onnxInitErr) {
			onnxInitErr = downloadFile(targetArchive, url)
			if onnxInitErr != nil {
				return errors.Wrap(onnxInitErr, "failed to download onnxruntime.tgz")
			}
			if _, err := os.Stat(targetArchive); err != nil {
				return errors.Wrap(err, "downloaded archive not found after download")
			}
			if err := verifyTarGzFile(targetArchive); err != nil {
				return errors.Wrap(err, "failed to verify downloaded onnxruntime archive")
			}
		}
	}
	targetFile := "onnxruntime-" + cos + "-" + carch + "-" + cfg.LibOnnxRuntimeVersion + "/lib/libonnxruntime." + cfg.LibOnnxRuntimeVersion + "." + getExtensionForOs()
	if cos == "linux" {
		targetFile = "onnxruntime-" + cos + "-" + carch + "-" + cfg.LibOnnxRuntimeVersion + "/lib/libonnxruntime." + getExtensionForOs() + "." + cfg.LibOnnxRuntimeVersion
	}
	onnxInitErr = extractSpecificFile(targetArchive, targetFile, cfg.OnnxCacheDir)
	if onnxInitErr != nil {
		return errors.Wrapf(onnxInitErr, "could not extract onnxruntime shared library")
	}

	if cos == "linux" {
		onnxInitErr = os.Rename(filepath.Join(cfg.OnnxCacheDir, "libonnxruntime."+getExtensionForOs()+"."+cfg.LibOnnxRuntimeVersion), cfg.OnnxLibPath)
		if onnxInitErr != nil {
			return errors.Wrapf(onnxInitErr, "could not rename extracted file to %s", cfg.OnnxLibPath)
		}
	}

	if _, err := os.Stat(cfg.OnnxLibPath); err != nil {
		return errors.Wrapf(err, "extracted file not found at expected location: %s", cfg.OnnxLibPath)
	}

	onnxInitErr = os.RemoveAll(targetArchive)
	if onnxInitErr != nil {
		return errors.Wrapf(onnxInitErr, "could not remove temporary archive: %s", targetArchive)
	}

	return onnxInitErr
}

func EnsureDefaultEmbeddingFunctionModel() error {
	cfg := getConfig()

	lockFile, err := lockFile(cfg.OnnxModelsCachePath)
	if err != nil {
		return errors.Wrap(err, "failed to acquire lock for onnx download")
	}
	defer func() {
		_ = unlockFile(lockFile)
	}()

	downloadAndExtractNeeded := false
	if _, err := os.Stat(cfg.OnnxModelCachePath); os.IsNotExist(err) {
		downloadAndExtractNeeded = true
		if err := os.MkdirAll(cfg.OnnxModelCachePath, 0755); err != nil {
			return errors.Wrap(err, "failed to create onnx model cache")
		}
	}
	if !downloadAndExtractNeeded {
		return nil
	}
	targetArchive := filepath.Join(cfg.OnnxModelsCachePath, "onnx.tar.gz")
	if _, err := os.Stat(targetArchive); os.IsNotExist(err) {
		if err := downloadFile(targetArchive, onnxModelDownloadEndpoint); err != nil {
			return errors.Wrap(err, "failed to download onnx model")
		}
		if err := verifyFileChecksum(targetArchive, onnxModelSHA256); err != nil {
			_ = os.Remove(targetArchive)
			return errors.Wrap(err, "onnx model integrity check failed")
		}
	}
	if err := extractSpecificFile(targetArchive, "", cfg.OnnxModelCachePath); err != nil {
		return errors.Wrapf(err, "could not extract onnx model")
	}

	// err := os.RemoveAll(targetArchive)
	// if err != nil {
	//	return err
	//}
	return nil
}
