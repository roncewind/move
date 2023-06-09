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
	"github.com/senzing/senzing-tools/help"
	"github.com/senzing/senzing-tools/helper"
	"github.com/senzing/senzing-tools/option"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	defaultDelayInSeconds            int    = 0
	defaultFileType                  string = ""
	defaultInputURL                  string = ""
	defaultLogLevel                  string = "error"
	defaultMonitoringPeriodInSeconds int    = 60
	defaultOutputURL                 string = ""
	defaultRecordMax                 int    = 0
	defaultRecordMin                 int    = 0
	defaultRecordMonitor             int    = 100000

	Use   string = "move"
	Short string = "Move records from one location to another."
	Long  string = `
	Welcome to move!
	This tool will move records from one place to another. It validates the records conform to the Generic Entity Specification.

	For example:

	move --input-url "file:///path/to/json/lines/file.jsonl" --output-url "amqp://guest:guest@192.168.6.96:5672"
	move --input-url "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" --output-url "amqp://guest:guest@192.168.6.96:5672"
`
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

// ----------------------------------------------------------------------------
// Command
// ----------------------------------------------------------------------------

// RootCmd represents the command.
var RootCmd = &cobra.Command{
	Use:     Use,
	Short:   Short,
	Long:    Long,
	PreRun:  PreRun,
	Run:     Run,
	Version: Version(),
}

// ----------------------------------------------------------------------------

// Used in construction of cobra.Command
func PreRun(cobraCommand *cobra.Command, args []string) {
	loadConfigurationFile(cobraCommand)
	loadOptions(cobraCommand)
	cobraCommand.SetVersionTemplate(constant.VersionTemplate)
}

// ----------------------------------------------------------------------------

// The core of this command
func Run(cmd *cobra.Command, args []string) {
	fmt.Println("Run with the following parameters:")
	for _, key := range viper.AllKeys() {
		fmt.Println("  - ", key, " = ", viper.Get(key))
	}
	setLogLevel()
	if viper.GetInt(option.DelayInSeconds) > 0 {
		fmt.Println(time.Now(), "Sleep for", viper.GetInt(option.DelayInSeconds), "seconds to let queues and databases settle down and come up.")
		time.Sleep(time.Duration(viper.GetInt(option.DelayInSeconds)) * time.Second)
	}

	ctx := context.Background()

	mover := &mover.MoverImpl{
		FileType:                  viper.GetString(option.InputFileType),
		InputURL:                  viper.GetString(option.InputURL),
		LogLevel:                  viper.GetString(option.LogLevel),
		MonitoringPeriodInSeconds: viper.GetInt(option.MonitoringPeriodInSeconds),
		OutputURL:                 viper.GetString(option.OutputURL),
		RecordMax:                 viper.GetInt(option.RecordMax),
		RecordMin:                 viper.GetInt(option.RecordMin),
		RecordMonitor:             viper.GetInt(option.RecordMonitor),
	}
	mover.Move(ctx)

}

// ----------------------------------------------------------------------------

// Used in construction of cobra.Command
func Version() string {
	return helper.MakeVersion(githubVersion, githubIteration)
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
	RootCmd.Flags().Int(option.DelayInSeconds, defaultDelayInSeconds, help.DelayInSeconds)
	RootCmd.Flags().String(option.InputFileType, defaultFileType, help.InputFileType)
	RootCmd.Flags().String(option.InputURL, defaultInputURL, help.InputURL)
	RootCmd.Flags().String(option.LogLevel, defaultLogLevel, fmt.Sprintf(help.LogLevel, envar.LogLevel))
	RootCmd.Flags().Int(option.MonitoringPeriodInSeconds, defaultMonitoringPeriodInSeconds, help.MonitoringPeriodInSeconds)
	RootCmd.Flags().String(option.OutputURL, defaultOutputURL, help.OutputURL)
	RootCmd.Flags().Int(option.RecordMax, defaultRecordMax, help.RecordMax)
	RootCmd.Flags().Int(option.RecordMin, defaultRecordMin, help.RecordMin)
	RootCmd.Flags().Int(option.RecordMonitor, defaultRecordMonitor, help.RecordMonitor)
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
		option.DelayInSeconds:            defaultDelayInSeconds,
		option.MonitoringPeriodInSeconds: defaultMonitoringPeriodInSeconds,
		option.RecordMax:                 defaultRecordMax,
		option.RecordMin:                 defaultRecordMin,
		option.RecordMonitor:             defaultRecordMonitor,
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
