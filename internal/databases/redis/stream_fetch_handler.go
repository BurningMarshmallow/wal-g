package redis

import (
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
	if (stat.Mode() & os.ModeCharDevice) == 0 {
	} else {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	err := downloadAndDecompressStream(folder, backupName)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

func downloadAndDecompressStream(folder storage.Folder, fileName string) error {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	backup := Backup{internal.NewBackup(baseBackupFolder, fileName)}

	decompressor := compression.FindDecompressor(utility.GetFileExtension(fileName))

	// Get reader
	archiveReader, _, err := internal.TryDownloadWALFile(baseBackupFolder, backup.Name)
	if err != nil {
		return err
	}

	// Decompress file
	err = internal.DecompressWALFile(&internal.EmptyWriteIgnorer{WriteCloser: os.Stdout}, archiveReader, decompressor)
	if err != nil {
		return err
	}

	utility.LoggedClose(os.Stdout, "")
	return nil
}
