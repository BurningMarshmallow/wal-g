package redis

import (
	"bytes"
	"encoding/json"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

func HandleStreamPush(uploader *Uploader) {
	// Configure folder
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	// Init backup process
	var stream io.Reader = os.Stdin
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		tracelog.InfoLogger.Println("Data is piped from stdin")
	} else {
		tracelog.ErrorLogger.Println("WARNING: stdin is terminal: operating in test mode!")
		stream = strings.NewReader("testtesttest")
	}
	baseName := "dump_" + time.Now().Format(time.RFC3339)
	compressionExtension := uploader.Compressor.FileExtension()
	backupName := path.Join(baseName, "stream.") + compressionExtension

	startTime := time.Now()
	err := uploader.uploadStream(backupName, stream)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
	endTime := time.Now()

	uploadStreamSentinel(&StreamSentinelDto{BackupName: baseName, StartTime: startTime, EndTime: endTime, CompressionExtension: compressionExtension}, uploader, baseName+utility.SentinelSuffix)
}

func (uploader *Uploader) uploadStream(backupName string, stream io.Reader) error {
	compressed := internal.CompressAndEncrypt(stream, uploader.Compressor, internal.ConfigureCrypter())

	err := uploader.Upload(backupName, compressed)

	return err
}

func uploadStreamSentinel(sentinelDto *StreamSentinelDto, uploader *Uploader, name string) error {
	dtoBody, err := json.Marshal(*sentinelDto)
	if err != nil {
		return err
	}

	uploadingErr := uploader.Upload(name, bytes.NewReader(dtoBody))
	if uploadingErr != nil {
		tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", name)
		return uploadingErr
	}
	return nil
}
