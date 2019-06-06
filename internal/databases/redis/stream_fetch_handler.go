package redis

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamFetch(backupName string, folder storage.Folder) {
	if backupName == "" || backupName == "LATEST" {
		latest, err := internal.GetLatestBackupName(folder)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("Unable to get latest backup %+v\n", err)
		}
		backupName = latest
	}

	tracelog.InfoLogger.Printf("Going to fetch backup with name %+v\n", backupName)
	stat, _ := os.Stdout.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	err := downloadAndDecompressStream(folder, backupName)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

func downloadAndDecompressStream(folder storage.Folder, backupName string) error {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)

	backup := Backup{internal.NewBackup(baseBackupFolder, backupName)}
	streamSentinel, err := backup.fetchStreamSentinel()
	if err != nil {
		return err
	}

	decompressor := compression.FindDecompressor(streamSentinel.CompressionExtension)

	// Get reader
	streamPath := baseBackupFolder.GetSubFolder(backupName)
	sentinelName := "stream." + streamSentinel.CompressionExtension
	tracelog.InfoLogger.Printf("Going to fetch backup at folder %+v\n", streamPath)
	tracelog.InfoLogger.Printf("Going to fetch sentinel at name %+v\n", sentinelName)
	archiveReader, exists, err := internal.TryDownloadWALFile(streamPath, sentinelName)
	if err != nil {
		return err
	}
	if !exists {
		return internal.NewArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backupName))
	}

	// Decompress file
	err = internal.DecompressWALFile(&internal.EmptyWriteIgnorer{WriteCloser: os.Stdout}, archiveReader, decompressor)
	if err != nil {
		return err
	}

	utility.LoggedClose(os.Stdout, "")
	return nil
}

func (backup *Backup) fetchStreamSentinel() (StreamSentinelDto, error) {
	sentinelDto := StreamSentinelDto{}
	backupReaderMaker := internal.NewStorageReaderMaker(backup.BaseBackupFolder,
		backup.GetStopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return sentinelDto, err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
}
