package redis

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/tracelog"

	"github.com/spf13/cobra"
)

const streamFetchShortDescription = "Fetches a backup from storage"

// streamFetchCmd represents the streamFetch command
var streamFetchCmd = &cobra.Command{
	Use:   "stream-fetch",
	Short: streamFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		redis.HandleStreamFetch(args[0], folder)
	},
}

func init() {
	RedisCmd.AddCommand(streamFetchCmd)
}
