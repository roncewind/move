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
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/roncewind/move/io/rabbitmq"
	"github.com/roncewind/szrecord"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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

	move --inputURL "file:///path/to/json/lines/file.jsonl" --outputURL "amqp://guest:guest@192.168.6.96:5672"
	move --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" --outputURL "amqp://guest:guest@192.168.6.96:5672"
`,

	// The core of this command:
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("start Run")
		fmt.Println("viper key list:")
		for _, key := range viper.AllKeys() {
			fmt.Println("  - ", key, " = ", viper.Get(key))
		}

		if viper.IsSet("inputURL") &&
			viper.IsSet("outputURL") &&
			viper.IsSet("exchange") &&
			viper.IsSet("inputQueue") {

			waitGroup.Add(2)
			recordchan := make(chan record, 10)
			go read(viper.GetString("inputURL"), recordchan)
			go write(viper.GetString("outputURL"), viper.GetString("exchange"), viper.GetString("inputQueue"), recordchan)
			waitGroup.Wait()
		} else {
			cmd.Help()
		}
	},
}

// ----------------------------------------------------------------------------
func read(urlString string, recordchan chan record) {

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
func write(urlString string, exchange string, queue string, recordchan chan record) {
	fmt.Println("Enter write")
	defer waitGroup.Done()
	fmt.Println("Write URL string: ", urlString)
	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	printURL(u)

	client := rabbitmq.NewClient(exchange, queue, urlString)
	defer client.Close()

	var record record
	var result bool
	// Wait for record to be assigned.
	record, result = <-recordchan
	for {
		if !result && len(recordchan) == 0 {
			// This means the channel is empty and closed.
			fmt.Println("recordchan closed")
			client.Close()
			return
		}

		//TODO: meaningful or random MessageId?
		if err := client.Push(record); err != nil {
			fmt.Println("Failed to publish record line: ", record.lineNumber)
			fmt.Println("ERROR: ", err)
			// avoid a tight loop
			time.Sleep(2 * 1000 * time.Millisecond)
		} else {
			// Wait for record to be assigned.
			record, result = <-recordchan
		}
	}
}

// ----------------------------------------------------------------------------
func writeXXX(urlString string, exchange string, queue string, recordchan chan record) {
	fmt.Println("Enter write")
	defer waitGroup.Done()
	fmt.Println("Write URL:")
	fmt.Println("URL string: ", urlString)
	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	printURL(u)

	conn, err := amqp.Dial(urlString)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,   // queue name
		q.Name,   // routing key
		exchange, // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind queue")

	i := 0
	for {
		i++
		// Wait for record to be assigned.
		record, result := <-recordchan

		if !result {
			// This means the channel is empty and closed.
			fmt.Println("recordchan closed")
			return
		}

		err = ch.Publish(
			exchange, // exchange
			q.Name,   // routing key
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				Body:         []byte(record.line),
				ContentType:  "text/plain",
				DeliveryMode: amqp.Persistent,
				MessageId:    fmt.Sprintf("%s-%d", record.fileName, record.lineNumber), //TODO: meaningful or random MessageId?
			})
		if err != nil {
			fmt.Println("Failed to publish record ", i)
			fmt.Println("ERROR: ", err)
		}
	}
}

// ----------------------------------------------------------------------------
func readJSONLResource(jsonURL string, recordchan chan record) {
	response, err := http.Get(jsonURL)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()

	scanner := bufio.NewScanner(response.Body)
	scanner.Split(bufio.ScanLines)

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
	}
	close(recordchan)
}

// ----------------------------------------------------------------------------
func readJSONLFile(jsonFile string, recordchan chan record) {
	file, err := os.Open(jsonFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

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
	// fmt.Println(m["k"][0])
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
	RootCmd.Flags().StringVarP(&exchange, "exchange", "", "", "Message queue exchange name")
	viper.BindPFlag("exchange", RootCmd.Flags().Lookup("exchange"))
	RootCmd.Flags().StringVarP(&fileType, "fileType", "", "", "file type override")
	viper.BindPFlag("fileType", RootCmd.Flags().Lookup("fileType"))
	RootCmd.Flags().StringVarP(&inputQueue, "inputQueue", "", "", "Senzing input queue name")
	viper.BindPFlag("inputQueue", RootCmd.Flags().Lookup("inputQueue"))
	RootCmd.Flags().StringVarP(&inputURL, "inputURL", "i", "", "input location")
	viper.BindPFlag("inputURL", RootCmd.Flags().Lookup("inputURL"))
	RootCmd.Flags().StringVarP(&logLevel, "logLevel", "", "", "set the logging level, default Error")
	viper.BindPFlag("logLevel", RootCmd.Flags().Lookup("logLevel"))
	RootCmd.Flags().StringVarP(&outputURL, "outputURL", "o", "", "output location")
	viper.BindPFlag("outputURL", RootCmd.Flags().Lookup("outputURL"))
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
	viper.SetEnvPrefix("senzing_tools")
	viper.BindEnv("exchange")
	viper.BindEnv("fileType")
	viper.BindEnv("inputQueue")
	viper.BindEnv("inputURL")
	viper.BindEnv("logLevel")
	viper.BindEnv("outputURL")

	viper.SetDefault("exchange", "senzing")
	viper.SetDefault("inputQueue", "senzing-input")
	viper.SetDefault("logLevel", "error")

	// setup local variables, in case they came from a config file
	//TODO:  why do I have to do this?  env vars and cmdline params get mapped
	//  automatically, this is only IF the var is in the config file
	exchange = viper.GetString("exchange")
	fileType = viper.GetString("fileType")
	inputQueue = viper.GetString("inputQueue")
	inputURL = viper.GetString("inputURL")
	logLevel = viper.GetString("logLevel")
	outputURL = viper.GetString("outputURL")

	setLogLevel()
}

// ----------------------------------------------------------------------------
func setLogLevel() {
	var level logger.Level = logger.LevelError
	if viper.IsSet("logLevel") {
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
