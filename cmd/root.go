package cmd

import (
	"context"
	"time"

	"fmt"
	"os"
	"strings"

	"github.com/docktermj/go-xyzzy-helpers/logger"
	"github.com/roncewind/move/mover"
	"github.com/senzing/senzing-tools/constant"
	"github.com/senzing/senzing-tools/envar"
	"github.com/senzing/senzing-tools/option"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	defaultDelayInSeconds int    = 0
	defaultFileType       string = ""
	defaultInputURL       string = ""
	defaultOutputURL      string = ""
	defaultLogLevel       string = "error"
)

const (
	envVarReplacerCharNew string = "_"
	envVarReplacerCharOld string = "-"
)

// move is 6202:  https://github.com/Senzing/knowledge-base/blob/main/lists/senzing-product-ids.md
const MessageIdFormat = "senzing-6202%04d"

var (
	buildIteration string = "0"
	buildVersion   string = "0.0.0"
	programName    string = fmt.Sprintf("move-%d", time.Now().Unix())
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "move",
	Short: "Move records from one location to another.",
	Long: `
	Welcome to move!
	This tool will move records from one place to another. It validates the records conform to the Generic Entity Specification.

	For example:

	move --input-url "file:///path/to/json/lines/file.jsonl" --output-url "amqp://guest:guest@192.168.6.96:5672"
	move --input-url "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" --output-url "amqp://guest:guest@192.168.6.96:5672"
`,
	PreRun: func(cobraCommand *cobra.Command, args []string) {
		loadConfigurationFile(cobraCommand)
		loadOptions(cobraCommand)
		cobraCommand.SetVersionTemplate(constant.VersionTemplate)
	},
	// The core of this command:
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("start Run")
		fmt.Println("viper key list:")
		for _, key := range viper.AllKeys() {
			fmt.Println("  - ", key, " = ", viper.Get(key))
		}
		setLogLevel()
		fmt.Println(time.Now(), "Sleep for", viper.GetInt(option.DelayInSeconds), "seconds to let RabbitMQ and Postgres settle down and come up.")
		time.Sleep(time.Duration(viper.GetInt(option.DelayInSeconds)) * time.Second)

		if viper.IsSet(option.InputURL) &&
			viper.IsSet(option.OutputURL) {

			ctx := context.Background()

			mover := &mover.MoverImpl{
				FileType:  viper.GetString(option.InputFileType),
				InputURL:  viper.GetString(option.InputURL),
				LogLevel:  viper.GetString(option.LogLevel),
				OutputURL: viper.GetString(option.OutputURL),
			}
			mover.Move(ctx)

		} else {
			cmd.Help()
			fmt.Println("Build Version:", buildVersion)
			fmt.Println("Build Iteration:", buildIteration)
		}
	},
}

// ----------------------------------------------------------------------------

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the RootCmd.
func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

// ----------------------------------------------------------------------------
func init() {
	RootCmd.Flags().Int(option.DelayInSeconds, defaultDelayInSeconds, option.DelayInSecondsHelp)
	RootCmd.Flags().String(option.InputFileType, defaultFileType, option.InputFileTypeHelp)
	RootCmd.Flags().String(option.InputURL, defaultInputURL, option.InputURLHelp)
	RootCmd.Flags().String(option.LogLevel, defaultLogLevel, fmt.Sprintf(option.LogLevelHelp, envar.LogLevel))
	RootCmd.Flags().String(option.OutputURL, defaultOutputURL, option.OutputURLHelp)
}

// ----------------------------------------------------------------------------

// If a configuration file is present, load it.
func loadConfigurationFile(cobraCommand *cobra.Command) {
	configuration := ""
	configFlag := cobraCommand.Flags().Lookup(option.Configuration)
	if configFlag != nil {
		configuration = configFlag.Value.String()
	}
	if configuration != "" { // Use configuration file specified as a command line option.
		viper.SetConfigFile(configuration)
	} else { // Search for a configuration file.

		// Determine home directory.

		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Specify configuration file name.

		viper.SetConfigName("move")
		viper.SetConfigType("yaml")

		// Define search path order.

		viper.AddConfigPath(home + "/.senzing-tools")
		viper.AddConfigPath(home)
		viper.AddConfigPath("/etc/senzing-tools")
	}

	// If a config file is found, read it in.

	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Applying configuration file:", viper.ConfigFileUsed())
	}
}

// ----------------------------------------------------------------------------

// Configure Viper with user-specified options.
func loadOptions(cobraCommand *cobra.Command) {
	viper.AutomaticEnv()
	replacer := strings.NewReplacer(envVarReplacerCharOld, envVarReplacerCharNew)
	viper.SetEnvKeyReplacer(replacer)
	viper.SetEnvPrefix(constant.SetEnvPrefix)

	// Ints

	intOptions := map[string]int{
		option.DelayInSeconds: defaultDelayInSeconds,
	}
	for optionKey, optionValue := range intOptions {
		viper.SetDefault(optionKey, optionValue)
		viper.BindPFlag(optionKey, cobraCommand.Flags().Lookup(optionKey))
	}

	// Strings

	stringOptions := map[string]string{
		option.InputFileType: defaultFileType,
		option.InputURL:      defaultInputURL,
		option.LogLevel:      defaultLogLevel,
		option.OutputURL:     defaultOutputURL,
	}
	for optionKey, optionValue := range stringOptions {
		viper.SetDefault(optionKey, optionValue)
		viper.BindPFlag(optionKey, cobraCommand.Flags().Lookup(optionKey))
	}

}

// ----------------------------------------------------------------------------
func setLogLevel() {
	var level logger.Level = logger.LevelError
	if viper.IsSet(option.LogLevel) {
		switch strings.ToUpper(viper.GetString(option.LogLevel)) {
		case logger.LevelDebugName:
			level = logger.LevelDebug
		case logger.LevelErrorName:
			level = logger.LevelError
		case logger.LevelFatalName:
			level = logger.LevelFatal
		case logger.LevelInfoName:
			level = logger.LevelInfo
		case logger.LevelPanicName:
			level = logger.LevelPanic
		case logger.LevelTraceName:
			level = logger.LevelTrace
		case logger.LevelWarnName:
			level = logger.LevelWarn
		}
		logger.SetLevel(level)
	}
}
