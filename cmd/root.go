/*
Copyright © 2022 Ron Lynn <dad@lynntribe.net>
*/
package cmd

import (
	"bufio"
	"encoding/json"
	"time"

	// "errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/docktermj/go-xyzzy-helpers/logger"
	"github.com/roncewind/move/io/rabbitmq"
	"github.com/roncewind/move/io/rabbitmq/managedproducer"
	"github.com/roncewind/szrecord"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	fileTypeParameter  string = "file-type"
	inputURLParameter  string = "input-url"
	logLevelParameter  string = "log-level"
	outputURLParameter string = "output-url"
)

var (
	buildIteration string = "0"
	buildVersion   string = "0.0.0"
	programName    string = "move"
)

var (
	cfgFile    string
	exchange   string = "senzing"
	fileType   string
	inputQueue string = "senzing_input"
	inputURL   string
	logLevel   string = "error"
	outputURL  string
	waitGroup  sync.WaitGroup
)

type record struct {
	line       string
	fileName   string
	lineNumber int
}

func (record record) GetMessage() string {
	return record.line
}

func (record record) GetMessageId() string {
	//TODO: meaningful or random MessageId?
	return fmt.Sprintf("%s-%d", record.fileName, record.lineNumber)
}

// move is 6202:  https://github.com/Senzing/knowledge-base/blob/main/lists/senzing-product-ids.md
const MessageIdFormat = "senzing-6202%04d"

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

	// The core of this command:
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("start Run")
		fmt.Println("viper key list:")
		for _, key := range viper.AllKeys() {
			fmt.Println("  - ", key, " = ", viper.Get(key))
		}

		if viper.IsSet(inputURLParameter) &&
			viper.IsSet(outputURLParameter) {

			waitGroup.Add(2)
			recordchan := make(chan rabbitmq.Record, 10)
			go read(viper.GetString(inputURLParameter), recordchan)
			go write(viper.GetString(outputURLParameter), recordchan)
			waitGroup.Wait()
		} else {
			cmd.Help()
			u, err := url.Parse(viper.GetString(outputURLParameter))
			if err != nil {
				panic(err)
			}
			printURL(u)
			fmt.Println("Build Version:", buildVersion)
			fmt.Println("Build Iteration:", buildIteration)
		}
	},
}

// ----------------------------------------------------------------------------
func read(urlString string, recordchan chan rabbitmq.Record) {

	defer waitGroup.Done()

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(urlString) < 5 {
		fmt.Printf("ERROR: check the inputURL parameter: %s\n", urlString)
		return
	}

	fmt.Println("inputURL: ", urlString)
	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	//printURL(u)
	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(fileType) == "JSONL" {
			fmt.Println("Reading as a JSONL file.")
			readJSONLFile(u.Path, recordchan)
		} else {
			valid := validate(u.Path)
			fmt.Println("Is valid JSON?", valid)
			close(recordchan)
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(fileType) == "JSONL" {
			fmt.Println("Reading as a JSONL resource.")
			readJSONLResource(u.Path, recordchan)
		} else {
			fmt.Println("If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else {
		msg := fmt.Sprintf("We don't handle %s input URLs.", u.Scheme)
		panic(msg)
	}
}

// ----------------------------------------------------------------------------
func write(urlString string, recordchan chan rabbitmq.Record) {
	fmt.Println("Enter write")
	defer waitGroup.Done()
	fmt.Println("Write URL string: ", urlString)
	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	printURL(u)

	<-managedproducer.StartManagedProducer(urlString, 0, recordchan)
	fmt.Println("So long and thanks for all the fish.")
	// client := rabbitmq.NewClient(exchangeName, queueName, urlString)
	// client := rabbitmq.Init(&rabbitmq.Client{
	// 	ExchangeName:   exchangeName,
	// 	QueueName:      queueName,
	// 	ReconnectDelay: 5 * time.Second,
	// 	ReInitDelay:    3 * time.Second,
	// 	ResendDelay:    1 * time.Second,
	// }, urlString)
	// defer client.Close()

	// for {
	// 	// Wait for record to be assigned.
	// 	record, result := <-recordchan

	// 	if !result && len(recordchan) == 0 {
	// 		// This means the channel is empty and closed.
	// 		fmt.Println("all records moved, recordchan closed")
	// 		return
	// 	}

	// 	if err := client.Push(record); err != nil {
	// 		fmt.Println("Failed to publish record:", record.GetMessageId())
	// 		fmt.Println("Error: ", err)
	// 	}
	// }
}

// ----------------------------------------------------------------------------
func readJSONLResource(jsonURL string, recordchan chan rabbitmq.Record) {
	response, err := http.Get(jsonURL)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()

	scanner := bufio.NewScanner(response.Body)
	scanner.Split(bufio.ScanLines)

	fmt.Println(time.Now(), "Start resource read.")
	i := 0
	for scanner.Scan() {
		i++
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := szrecord.Validate(str)
			if valid {
				recordchan <- record{str, jsonURL, i}
			} else {
				fmt.Println("Line", i, err)
			}
		}
		if i%10000 == 0 {
			fmt.Println(time.Now(), "Records sent to queue:", i)
		}
	}
	close(recordchan)
}

// ----------------------------------------------------------------------------
func readJSONLFile(jsonFile string, recordchan chan rabbitmq.Record) {
	file, err := os.Open(jsonFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	fmt.Println(time.Now(), "Start file read.")
	i := 0
	for scanner.Scan() {
		i++
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := szrecord.Validate(str)
			if valid {
				recordchan <- record{str, jsonFile, i}
			} else {
				fmt.Println("Line", i, err)
			}
		}
		if i%10000 == 0 {
			fmt.Println(time.Now(), "Records sent to queue:", i)
		}
	}
	close(recordchan)
}

// ----------------------------------------------------------------------------
func validate(jsonFile string) bool {

	var file *os.File = os.Stdin

	if jsonFile != "" {
		var err error
		file, err = os.Open(jsonFile)
		if err != nil {
			log.Fatal(err)
		}
	}
	info, err := file.Stat()
	if err != nil {
		panic(err)
	}
	if info.Size() <= 0 {
		log.Fatal("No file found to validate.")
	}
	printFileInfo(info)

	bytes := GetBytes(file)
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}

	valid := json.Valid(bytes)
	return valid
}

// ----------------------------------------------------------------------------
func GetBytes(file *os.File) []byte {

	reader := bufio.NewReader(file)
	var output []byte

	for {
		input, err := reader.ReadByte()
		if err != nil && err == io.EOF {
			break
		}
		output = append(output, input)
	}
	return output
}

// ----------------------------------------------------------------------------
func printFileInfo(info os.FileInfo) {
	fmt.Println("name: ", info.Name())
	fmt.Println("size: ", info.Size())
	fmt.Println("mode: ", info.Mode())
	fmt.Println("mod time: ", info.ModTime())
	fmt.Println("is dir: ", info.IsDir())
	if info.Mode()&os.ModeDevice == os.ModeDevice {
		fmt.Println("detected device: ", os.ModeDevice)
	}
	if info.Mode()&os.ModeCharDevice == os.ModeCharDevice {
		fmt.Println("detected char device: ", os.ModeCharDevice)
	}
	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
		fmt.Println("detected named pipe: ", os.ModeNamedPipe)
	}
}

// ----------------------------------------------------------------------------
func printURL(u *url.URL) {

	fmt.Println("\tScheme: ", u.Scheme)
	fmt.Println("\tUser full: ", u.User)
	fmt.Println("\tUser name: ", u.User.Username())
	p, _ := u.User.Password()
	fmt.Println("\tPassword: ", p)

	fmt.Println("\tHost full: ", u.Host)
	host, port, _ := net.SplitHostPort(u.Host)
	fmt.Println("\tHost: ", host)
	fmt.Println("\tPort: ", port)

	fmt.Println("\tPath: ", u.Path)
	fmt.Println("\tFragment: ", u.Fragment)

	fmt.Println("\tQuery string: ", u.RawQuery)
	m, _ := url.ParseQuery(u.RawQuery)
	fmt.Println("\tParsed query string: ", m)
	for key, value := range m {
		fmt.Println("Key:", key, "=>", "Value:", value[0])
	}

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
	RootCmd.Flags().StringVarP(&fileType, fileTypeParameter, "", "", "file type override")
	viper.BindPFlag(fileTypeParameter, RootCmd.Flags().Lookup(fileTypeParameter))
	RootCmd.Flags().StringVarP(&inputURL, inputURLParameter, "i", "", "input location")
	viper.BindPFlag(inputURLParameter, RootCmd.Flags().Lookup(inputURLParameter))
	RootCmd.Flags().StringVarP(&logLevel, logLevelParameter, "", "", "set the logging level, default Error")
	viper.BindPFlag(logLevelParameter, RootCmd.Flags().Lookup(logLevelParameter))
	RootCmd.Flags().StringVarP(&outputURL, outputURLParameter, "o", "", "output location")
	viper.BindPFlag(outputURLParameter, RootCmd.Flags().Lookup(outputURLParameter))
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
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.SetEnvPrefix("SENZING_TOOLS")
	viper.BindEnv(fileTypeParameter)
	viper.BindEnv(inputURLParameter)
	viper.BindEnv(logLevelParameter)
	viper.BindEnv(outputURLParameter)

	viper.SetDefault(logLevelParameter, "error")

	// setup local variables, in case they came from a config file
	//TODO:  why do I have to do this?  env vars and cmdline params get mapped
	//  automatically, this is only IF the var is in the config file
	fileType = viper.GetString(fileTypeParameter)
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
