package generate

import (
	"github.com/spf13/cobra"

	"github.com/openfga/openfga/cmd/generate/index"
	"github.com/openfga/openfga/cmd/generate/sql"
)

func NewGenerateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "FGA generation utilities",
		Args:  cobra.NoArgs,
	}

	generateIndexCmd := index.NewGenerateIndexCommand()
	cmd.AddCommand(generateIndexCmd)

	generateSQLCmd := sql.NewGenerateSQLCommand()
	cmd.AddCommand(generateSQLCmd)

	return cmd
}
