package cmd

import (
	"time"

	"fmt"
	"os"
	"strings"

	"github.com/docktermj/go-xyzzy-helpers/logger"
	"github.com/roncewind/move/mover"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	delayInSeconds         string = "delay-in-seconds"
	envVarPrefix           string = "SENZING_TOOLS"
	envVarReplacerCharNew  string = "_"
	envVarReplacerCharOld  string = "-"
	inputFileTypeParameter string = "input-file-type"
	inputURLParameter      string = "input-url"
	logLevelParameter      string = "log-level"
	outputURLParameter     string = "output-url"
)

// move is 6202:  https://github.com/Senzing/knowledge-base/blob/main/lists/senzing-product-ids.md
const MessageIdFormat = "senzing-6202%04d"

var (
	buildIteration string = "0"
	buildVersion   string = "0.0.0"
	programName    string = "move"
)

var (
	delay      int = 0
	cfgFile    string
	exchange   string = "senzing"
	fileType   string
	inputQueue string = "senzing_input"
	inputURL   string
	logLevel   string = "error"
	outputURL  string
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
	PreRun: func(cmd *cobra.Command, args []string) {
		viper.BindPFlag(delayInSeconds, cmd.Flags().Lookup(delayInSeconds))
		viper.BindPFlag(inputFileTypeParameter, cmd.Flags().Lookup(inputFileTypeParameter))
		viper.BindPFlag(inputURLParameter, cmd.Flags().Lookup(inputURLParameter))
		viper.BindPFlag(logLevelParameter, cmd.Flags().Lookup(logLevelParameter))
		viper.BindPFlag(outputURLParameter, cmd.Flags().Lookup(outputURLParameter))
	},
	// The core of this command:
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("start Run")
		fmt.Println("viper key list:")
		for _, key := range viper.AllKeys() {
			fmt.Println("  - ", key, " = ", viper.Get(key))
		}
		fmt.Println(time.Now(), "Sleep for", delay, "seconds to let RabbitMQ and Postgres settle down and come up.")
		time.Sleep(time.Duration(delay) * time.Second)

		if viper.IsSet(inputURLParameter) &&
			viper.IsSet(outputURLParameter) {

			mover := &mover.MoverImpl{
				FileType:  viper.GetString(inputFileTypeParameter),
				InputURL:  viper.GetString(inputURLParameter),
				LogLevel:  viper.GetString(logLevelParameter),
				OutputURL: viper.GetString(outputURLParameter),
			}
			mover.Move()

		} else {
			cmd.Help()
			fmt.Println("Build Version:", buildVersion)
			fmt.Println("Build Iteration:", buildIteration)
		}
	},
}

// ----------------------------------------------------------------------------
func failOnError(err error, msg string) {
	if err != nil {
		s := fmt.Sprintf("%s: %s", msg, err)
		panic(s)
	}
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
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.senzing-tools/config.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	RootCmd.Flags().IntVarP(&delay, delayInSeconds, "", 0, "time to wait before start of processing")
	RootCmd.Flags().StringVarP(&fileType, inputFileTypeParameter, "", "", "file type override")
	RootCmd.Flags().StringVarP(&inputURL, inputURLParameter, "i", "", "input location")
	RootCmd.Flags().StringVarP(&logLevel, logLevelParameter, "", "", "set the logging level, default Error")
	RootCmd.Flags().StringVarP(&outputURL, outputURLParameter, "o", "", "output location")
}

// ----------------------------------------------------------------------------
// initConfig reads in config file and ENV variables if set.
// Config precedence:
// - cmdline args
// - env vars
// - config file
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in <home directory>/.senzing with name "config" (without extension).
		viper.AddConfigPath(home + "/.senzing-tools")
		viper.AddConfigPath(home)
		viper.AddConfigPath("/etc/senzing-tools")
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error
		} else {
			// Config file was found but another error was produced
			logger.LogMessageFromError(MessageIdFormat, 2001, "Config file found, but not loaded", err)
		}
	}
	viper.AutomaticEnv() // read in environment variables that match
	// all env vars should be prefixed with "SENZING_TOOLS_"
	replacer := strings.NewReplacer(envVarReplacerCharOld, envVarReplacerCharNew)
	viper.SetEnvKeyReplacer(replacer)
	viper.SetEnvPrefix(envVarPrefix)
	viper.BindEnv(delayInSeconds)
	viper.BindEnv(inputFileTypeParameter)
	viper.BindEnv(inputURLParameter)
	viper.BindEnv(logLevelParameter)
	viper.BindEnv(outputURLParameter)

	viper.SetDefault(delayInSeconds, 0)
	viper.SetDefault(logLevelParameter, "error")

	// setup local variables, in case they came from a config file
	//TODO:  why do I have to do this?  env vars and cmdline params get mapped
	//  automatically, this is only IF the var is in the config file
	delay = viper.GetInt(delayInSeconds)
	fileType = viper.GetString(inputFileTypeParameter)
	inputURL = viper.GetString(inputURLParameter)
	logLevel = viper.GetString(logLevelParameter)
	outputURL = viper.GetString(outputURLParameter)

	setLogLevel()
}

// ----------------------------------------------------------------------------
func setLogLevel() {
	var level logger.Level = logger.LevelError
	if viper.IsSet(logLevelParameter) {
		switch strings.ToUpper(logLevel) {
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
