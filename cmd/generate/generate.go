package generate

import (
	"github.com/spf13/cobra"

	"github.com/openfga/openfga/cmd/generate/index"
)

func NewGenerateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "FGA generation utilities",
		Args:  cobra.NoArgs,
	}

	generateIndexCmd := index.NewGenerateIndexCommand()
	cmd.AddCommand(generateIndexCmd)

	return cmd
}
