package index

import (
	"context"
	"fmt"
	"log"
	"os"

	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/spf13/cobra"

	"github.com/openfga/openfga/internal/materializer"

	"github.com/openfga/openfga/pkg/typesystem"
)

func NewGenerateIndexCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "FGA Index materialization generator",
		Run:   generateIndex,
		Args:  cobra.NoArgs,
	}

	flags := cmd.Flags()

	flags.String("name", "", "a unique name for the index")

	flags.String("file", "model.fga", "an absolute file path to an FGA model (default 'model.fga')")
	flags.String("output", "", "an absolute file path to the output file")

	flags.String("dialect", "materialize", "the SQL dialect to target for SQL production ('postgresql', 'mysql', 'materialize')")

	return cmd
}

func generateIndex(cmd *cobra.Command, args []string) {
	indexName, err := cmd.Flags().GetString("name")
	if err != nil {
		log.Fatalf("'name' is a required flag")
	}

	modelFile, err := cmd.Flags().GetString("file")
	if err != nil {
		log.Fatalf("'file' is a required flag")
	}

	outputFilePath, err := cmd.Flags().GetString("output")
	if err != nil {
		log.Fatalf("'output' is a required flag")
	}

	dialect, err := cmd.Flags().GetString("dialect")
	if err != nil {
		log.Fatalf("'dialect' is a required flag")
	}

	modelBytes, err := os.ReadFile(modelFile)
	if err != nil {
		log.Fatalf("failed to read the provided FGA model file: %v", err)
	}

	model := parser.MustTransformDSLToProto(string(modelBytes))

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	if err != nil {
		log.Fatalf("the provided FGA model is invalid: %v", err)
	}

	sql, err := materializer.Materialize(materializer.MaterializerInput{
		IndexName:  indexName,
		Typesystem: typesys,
		Dialect:    materializer.MaterializationDialect(dialect),
	})
	if err != nil {
		log.Fatalf("failed to Materialize the provided FGA model: %v", err)
	}

	if outputFilePath != "" {
		outputFile, err := os.Create(outputFilePath)
		if err != nil {
			log.Fatalf("failed to open output file '%s': %v", outputFilePath, err)
		}

		_, err = outputFile.WriteString(sql)
		if err != nil {
			log.Fatalf("failed to write to output file: %v", err)
		}
	} else {
		fmt.Println(sql)
	}
}
